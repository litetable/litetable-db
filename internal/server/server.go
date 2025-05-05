package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/engine"
	"github.com/rs/zerolog/log"
	"net"
	"sync"
	"time"
)

const (
	serverName = "Litetable Server"
)

type handler interface {
	Handle(conn engine.Conn)
}

type Server struct {
	certificate tls.Certificate
	listener    net.Listener
	port        string
	address     string
	handler     handler

	// configuration for handling connections
	maxConnections int
	connSemaphore  chan struct{}
	activeConns    sync.WaitGroup
	enableTLS      bool
}

type Config struct {
	Certificate    *tls.Certificate
	Address        string
	Port           string
	Handler        handler
	MaxConnections int
	EnableTLS      bool
}

func (c *Config) validate() error {
	var errGrp []error

	if c.Certificate == nil {
		errGrp = append(errGrp, errors.New("certificate is required"))
	}
	if c.Address == "" {
		errGrp = append(errGrp, errors.New("address is required"))
	}
	if c.Port == "" {
		errGrp = append(errGrp, errors.New("port is required"))
	}
	if c.Handler == nil {
		errGrp = append(errGrp, errors.New("handler is required"))
	}

	return errors.Join(errGrp...)
}

// New returns a new Litetable server, which provides a way to start and listen to
// incoming LT transactions.
func New(cfg *Config) (*Server, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	var listener net.Listener
	var err error

	connAddress := fmt.Sprintf("%s:%s", cfg.Address, cfg.Port)
	if cfg.EnableTLS {
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{*cfg.Certificate},
			MinVersion:   tls.VersionTLS12,
		}
		listener, err = tls.Listen("tcp", connAddress, tlsConfig)
	} else {
		listener, err = net.Listen("tcp", connAddress)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %w", err)
	}

	maxConns := cfg.MaxConnections
	if maxConns <= 0 {
		maxConns = 100 // default value
	}

	return &Server{
		certificate:    *cfg.Certificate,
		listener:       listener,
		address:        cfg.Address,
		port:           cfg.Port,
		handler:        cfg.Handler,
		maxConnections: maxConns,
		connSemaphore:  make(chan struct{}, maxConns), // Initialize the channel
		enableTLS:      cfg.EnableTLS,
		activeConns:    sync.WaitGroup{},
	}, nil
}

func (s *Server) Start() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		remoteAddr := conn.RemoteAddr().String()

		// Try to acquire a connection slot
		select {
		case s.connSemaphore <- struct{}{}: // Connection slot acquired
			s.activeConns.Add(1)
			go func() {
				defer func() {
					<-s.connSemaphore // Release the connection slot
					s.activeConns.Done()
				}()
				start := time.Now()
				log.Debug().Str("addr", remoteAddr).Msgf("incoming connection: %s", remoteAddr)
				s.handler.Handle(conn)
				log.Debug().Str("addr", remoteAddr).Str("duration", time.Since(start).String()).Msgf("connection closed: %s", remoteAddr)
			}()
		default:
			// Max connections reached, reject the connection
			_ = conn.Close()
			log.Warn().Msgf("Rejected connection from %s: max connections reached", remoteAddr)
		}
	}
}

// Stop will stop the server from accepting new connections.
func (s *Server) Stop() error {
	err := s.listener.Close()
	s.activeConns.Wait() // Wait for all active connections to finish
	return err
}

// Name returns the name of the server.
func (s *Server) Name() string {
	return serverName
}
