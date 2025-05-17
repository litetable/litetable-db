package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

//go:generate mockgen -destination ./server_mock.go -package server -source ./server.go

type httpServer interface {
	ListenAndServe() error
	Shutdown(ctx context.Context) error
	Addr() string
}

type realHTTPServer struct {
	s *http.Server
}

type Server struct {
	address string
	port    int
	router  *http.ServeMux
	server  httpServer // Add this field
}

type Config struct {
	Address string
	Port    int
}

// validate checks the configuration for any errors
func (c *Config) validate() error {
	var errGrp []error
	if c.Address == "" {
		errGrp = append(errGrp, fmt.Errorf("address is required"))
	}
	if c.Port <= 0 || c.Port > 65535 {
		errGrp = append(errGrp, fmt.Errorf("port must be between 1 and 65535"))
	}
	return errors.Join(errGrp...)
}

func New(cfg *Config) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// init an http server
	mux := http.NewServeMux()

	server := &http.Server{
		Addr: fmt.Sprintf("%s:%d", cfg.Address, cfg.Port),
	}

	m := &Server{
		address: cfg.Address,
		port:    cfg.Port,
		server:  &realHTTPServer{s: server},
	}
	mux.HandleFunc("GET /health", m.Health)
	server.Handler = mux

	return m, nil
}

func (s *Server) Start() error {
	log.Info().Msgf("HTTP server listening on %s", s.server.Addr())

	errCh := make(chan error, 1)

	go func() {
		if err := s.server.ListenAndServe(); err != nil {
			errCh <- err // always return any ListenAndServe error, even ErrServerClosed
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
	// Graceful shutdown with timeout
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.server.Shutdown(ctx); err != nil {
			return fmt.Errorf("server shutdown failed: %w", err)
		}

		log.Info().Msg("HTTP server stopped gracefully")
	}

	// Shutdown the server
	return nil
}

func (s *Server) Name() string {
	return "LiteTable http server"
}

func (s *Server) Health(w http.ResponseWriter, r *http.Request) {
	log.Debug().Msg("incoming health check")
	// Handle HTTP requests here
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := `{"status": "ok"}`
	log.Debug().Msg("Health check response: " + response)
	_, _ = w.Write([]byte(response))
}

func (r *realHTTPServer) ListenAndServe() error {
	return r.s.ListenAndServe()
}

func (r *realHTTPServer) Shutdown(ctx context.Context) error {
	return r.s.Shutdown(ctx)
}

func (r *realHTTPServer) Addr() string {
	return r.s.Addr
}
