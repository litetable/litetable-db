package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
)

const (
	serverName = "Litetable Server"
)

type handler interface {
	Handle(conn net.Conn)
}

type Server struct {
	certificate tls.Certificate
	listener    net.Listener
	port        string
	handler     handler

	// configuration for handling connections
	maxConnections int
	connSemaphore  chan struct{}
	activeConns    sync.WaitGroup
	enableTLS      bool
}

type Config struct {
	Certificate    *tls.Certificate
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
	if cfg.EnableTLS {
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{*cfg.Certificate},
			MinVersion:   tls.VersionTLS12,
		}
		listener, err = tls.Listen("tcp", ":"+cfg.Port, tlsConfig)
	} else {
		listener, err = net.Listen("tcp", ":"+cfg.Port)
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

				fmt.Printf("Handling connection from: %s\n", remoteAddr)
				s.handler.Handle(conn)
			}()
		default:
			// Max connections reached, reject the connection
			_ = conn.Close()
			fmt.Printf("Rejected connection from %s: max connections reached\n", remoteAddr)
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
