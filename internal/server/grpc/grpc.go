package grpc

import (
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/rs/zerolog/log"
	grpc2 "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"time"
)

// Server implements the app.Dependency interface for a gRPC server
type Server struct {
	address  string
	server   grpcServer
	port     int
	listener net.Listener
}

type Config struct {
	Address    string
	Port       int
	Operations operations
}

func (c *Config) validate() error {
	var errGrp []error
	if c.Address == "" {
		errGrp = append(errGrp, fmt.Errorf("address required"))
	}
	if c.Port == 0 {
		errGrp = append(errGrp, fmt.Errorf("port required"))
	}
	if c.Operations == nil {
		errGrp = append(errGrp, fmt.Errorf("operations required"))
	}

	return errors.Join(errGrp...)
}

// NewServer creates a new gRPC server instance
func NewServer(cfg *Config) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// Create a new gRPC server
	srv := grpc2.NewServer()

	l := &lt{
		operations: cfg.Operations,
	}

	srv.RegisterService(&proto.LitetableService_ServiceDesc, l)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Address, cfg.Port))
	if err != nil {
		return nil, fmt.Errorf("failed to create listener on port %d: %w", cfg.Port, err)
	}

	return &Server{
		address:  cfg.Address,
		server:   srv,
		port:     cfg.Port,
		listener: lis,
	}, nil
}

func (s *Server) Start() error {
	log.Info().Msgf("gRPC server listening at %s:%d", s.address, s.port)

	errCh := make(chan error, 1)

	// Start server in a goroutine
	go func() {
		if err := s.server.Serve(s.listener); err != nil {
			errCh <- err
			log.Error().Err(err).Msg("gRPC server failed")
			return
		}
		errCh <- nil
	}()

	// Block briefly for error or nil return
	select {
	case err := <-errCh:
		return err
	case <-time.After(500 * time.Millisecond):
		// Assume server started successfully
		return nil
	}
}

func (s *Server) Stop() error {
	log.Info().Msg("Stopping gRPC server")
	s.server.GracefulStop()
	return nil
}

func (s *Server) Name() string {
	return "gRPC Server"
}
