package grpc

import (
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/rs/zerolog/log"
	grpc2 "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

// Server implements the app.Dependency interface for a gRPC server
type Server struct {
	address string
	server  *grpc2.Server
	port    int
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

	lt := &litetable{
		operations: cfg.Operations,
	}

	srv.RegisterService(&proto.LitetableService_ServiceDesc, lt)

	reflection.Register(srv)
	return &Server{
		address: cfg.Address,
		server:  srv,
		port:    cfg.Port,
	}, nil
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.address, s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}

	log.Info().Msgf("gRPC server listening at %s:%d", s.address, s.port)

	// Start server in a goroutine
	go func() {
		if err := s.server.Serve(lis); err != nil {
			log.Error().Err(err).Msg("gRPC server failed")
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	log.Info().Msg("Stopping gRPC server")
	s.server.GracefulStop()
	return nil
}

func (s *Server) Name() string {
	return "gRPC Server"
}
