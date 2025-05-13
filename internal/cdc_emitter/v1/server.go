package v1

import (
	"fmt"
	v1 "github.com/litetable/litetable-cdc/go/v1"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"net"
	"sync"
)

const (
	cdcAddress = "127.0.0.1"
	cdcPort    = 32473
)

type Server struct {
	v1.UnimplementedCDCServiceServer
	address     string
	port        int
	grpcStreams map[string]v1.CDCService_CDCStreamServer
	grpcMux     sync.Mutex

	server *grpc.Server
	events chan *CDCEvent
}

func New() *Server {
	cdcServer := &Server{
		address:     cdcAddress,
		port:        cdcPort,
		grpcStreams: make(map[string]v1.CDCService_CDCStreamServer),
		events:      make(chan *CDCEvent, 1000),
	}

	// Create a new gRPC server
	srv := grpc.NewServer()

	// Register the CDC service
	v1.RegisterCDCServiceServer(srv, cdcServer)

	cdcServer.server = srv
	return cdcServer
}

type grpcSubscriber struct {
	id     string
	stream v1.CDCService_CDCStreamServer
	done   chan struct{}
}

var grpcSubscribers sync.Map // map[string]*grpcSubscriber

func (s *Server) CDCStream(req *v1.CDCSubscriptionRequest,
	stream v1.CDCService_CDCStreamServer) error {
	sub := &grpcSubscriber{
		id:     req.GetClientId(),
		stream: stream,
		done:   make(chan struct{}),
	}

	grpcSubscribers.Store(sub.id, sub)

	// Optional: Replay past events
	if req.GetReplay() {
		// TODO: Implement replay logic one day
	}

	// Register with emitter to receive live events
	s.registerGRPCStream(sub.id, sub.stream)

	<-sub.done // block until client disconnects
	grpcSubscribers.Delete(sub.id)
	s.unregisterGRPCStream(sub.id)
	return nil
}

func (s *Server) registerGRPCStream(clientID string, stream v1.CDCService_CDCStreamServer) {
	s.grpcMux.Lock()
	defer s.grpcMux.Unlock()
	if s.grpcStreams == nil {
		s.grpcStreams = make(map[string]v1.CDCService_CDCStreamServer)
	}
	s.grpcStreams[clientID] = stream
}

func (s *Server) unregisterGRPCStream(clientID string) {
	s.grpcMux.Lock()
	defer s.grpcMux.Unlock()
	delete(s.grpcStreams, clientID)
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.address, s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}

	log.Info().Msgf("CDC gRPC server listening at %s:%d", s.address, s.port)

	// Start fan-out dispatcher
	go s.dispatchLoop()

	// Start gRPC server
	go func() {
		if err := s.server.Serve(lis); err != nil {
			log.Error().Err(err).Msg("CDC gRPC server failed")
		}
	}()

	return nil
}
func (s *Server) Stop() error { return nil }

func (s *Server) Name() string {
	return "CDC Stream"
}

func (s *Server) dispatchLoop() {
	for evt := range s.events {
		s.grpcMux.Lock()
		for id, stream := range s.grpcStreams {
			event := &v1.CDCEvent{
				RowKey:        evt.RowKey,
				Family:        evt.Family,
				Qualifier:     evt.Qualifier,
				Value:         evt.Value,
				TimestampUnix: evt.Timestamp,
				Tombstone:     evt.IsTombstone,
				ExpiresAtUnix: evt.ExpiresAt,
			}

			switch evt.Operation {
			case litetable.OperationRead:
				event.Operation = v1.LitetableOperation_READ
			case litetable.OperationWrite:
				event.Operation = v1.LitetableOperation_WRITE
			case litetable.OperationDelete:
				event.Operation = v1.LitetableOperation_DELETE
			}

			err := stream.Send(event)
			if err != nil {
				log.Warn().Err(err).Str("client", id).Msg("removing gRPC stream due to send error")
				delete(s.grpcStreams, id)
			}
		}
		s.grpcMux.Unlock()
	}
}
