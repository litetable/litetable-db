package server

import (
	"crypto/tls"
	"net"
)

type handler interface {
	Handle(conn net.Conn)
}

type Server struct {
	certificate tls.Certificate
	listener    net.Listener
	port        string
	handler     handler
}

type Config struct {
	Certificate tls.Certificate
	Port        string
	Handler     handler
}

// New returns a new Litetable server, which provides a way to start and listen to
// incoming LT transactions.
func New(cfg *Config) (*Server, error) {

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cfg.Certificate},
	}
	listener, err := tls.Listen("tcp", ":"+cfg.Port, tlsConfig)
	if err != nil {
		return nil, err
	}

	return &Server{
		certificate: cfg.Certificate,
		listener:    listener,
		port:        cfg.Port,
	}, nil
}

func (s *Server) Start() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		// call the provided callback
		go s.handler.Handle(conn)
	}
}

// Stop will stop the server from accepting new connections.
func (s *Server) Stop() error {
	return s.listener.Close()
}

func (s *Server) Name() string {
	return "Litetable Server"
}
