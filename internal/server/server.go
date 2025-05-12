package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

type Manager struct {
	address string
	port    int
	router  *http.ServeMux
	server  *http.Server // Add this field
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

func New(cfg *Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// init an http server
	srv := http.NewServeMux()

	server := &http.Server{
		Addr: fmt.Sprintf("%s:%d", cfg.Address, cfg.Port),
	}

	// create a new server
	m := &Manager{
		address: cfg.Address,
		port:    cfg.Port,
		server:  server,
	}
	srv.HandleFunc("GET /health", m.Health)
	m.server.Handler = srv

	return m, nil
}

func (m *Manager) Start() error {
	log.Info().Msgf("HTTP server listening on %s", m.server.Addr)
	// Run the server in a separate goroutine
	go func() {
		if err := m.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("HTTP server failed")
		}
	}()
	return nil
}

func (m *Manager) Stop() error {
	// Graceful shutdown with timeout
	if m.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := m.server.Shutdown(ctx); err != nil {
			return fmt.Errorf("server shutdown failed: %w", err)
		}

		log.Info().Msg("HTTP server stopped gracefully")
	}

	// Shutdown the server
	return nil
}

func (m *Manager) Name() string {
	return "http server"
}

func (m *Manager) Health(w http.ResponseWriter, r *http.Request) {
	log.Debug().Msg("incoming health check")
	// Handle HTTP requests here
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := `{"status": "ok"}`
	log.Debug().Msg("Health check response: " + response)
	_, _ = w.Write([]byte(response))
}
