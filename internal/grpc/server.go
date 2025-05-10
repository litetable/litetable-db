package grpc

import (
	"fmt"
	"github.com/rs/zerolog/log"
	grpc2 "google.golang.org/grpc"
	"net"
)

// Server implements the app.Dependency interface for a gRPC server
type Server struct {
	server *grpc2.Server
	port   int
}

type Config struct {
	Port int
}

// NewServer creates a new gRPC server instance
func NewServer(cfg *Config) (*Server, error) {
	// Create a new gRPC server
	srv := grpc2.NewServer()

	// Register your gRPC services here
	// For example:
	// proto.RegisterYourServiceServer(srv, &YourServiceServer{})
	// Register the server with the gRPC server
	// proto.RegisterLitetableServiceServer(srv, &Litetable{})

	return &Server{
		server: srv,
		port:   cfg.Port,
	}, nil
}
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}

	log.Info().Msgf("gRPC server listening on port %d", s.port)

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
