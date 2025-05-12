package cdc_emitter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

type Config struct {
	Port    int
	Address string
}

func (c *Config) validate() error {
	var errGrp []error
	if c.Port <= 0 {
		errGrp = append(errGrp, fmt.Errorf("invalid port: %d", c.Port))
	}
	if c.Address == "" {
		errGrp = append(errGrp, fmt.Errorf("invalid address: %s", c.Address))
	}
	return errors.Join(errGrp...)
}

type Manager struct {
	port     int
	address  string
	listener net.Listener

	emitChan   chan *CDCParams
	procCtx    context.Context
	procCancel context.CancelFunc

	clients    map[net.Conn]bool
	clientsMux sync.Mutex
}

func New(cfg *Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	addrString := fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)
	listener, err := net.Listen("tcp", addrString)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addrString, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		listener:   listener,
		port:       cfg.Port,
		address:    cfg.Address,
		emitChan:   make(chan *CDCParams, 100000),
		procCtx:    ctx,
		procCancel: cancel,

		clients:    make(map[net.Conn]bool),
		clientsMux: sync.Mutex{},
	}, nil
}

func (m *Manager) Start() error {
	go func() {
		for {
			select {
			case <-m.procCtx.Done():
				return
			case p := <-m.emitChan:
				m.raiseCDCEvent(p)
			}
		}
	}()

	// Start the listener in a separate goroutine
	go func() {
		for {
			select {
			case <-m.procCtx.Done():
				return
			default:
				conn, err := m.listener.Accept()
				if err != nil {
					fmt.Printf("Failed to accept connection: %v\n", err)
					continue
				}

				go m.handle(conn) // Handle the connection in a separate goroutine
			}
		}
	}()

	return nil
}

func (m *Manager) Stop() error {
	// Close the listener
	if m.listener != nil {
		err := m.listener.Close()
		if err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}

	if m.procCancel != nil {
		m.procCancel()
	}

	return nil
}

func (m *Manager) Name() string {
	return "CDC Emitter"
}

func (m *Manager) handle(conn net.Conn) {
	defer func() {
		// Clean up when the connection ends
		err := conn.Close()
		if err != nil {
			return
		}

		m.clientsMux.Lock()
		delete(m.clients, conn)
		m.clientsMux.Unlock()
	}()

	// Register this client
	m.clientsMux.Lock()
	m.clients[conn] = true
	m.clientsMux.Unlock()

	fmt.Println("Client connected:", conn.RemoteAddr())

	// Keep the connection alive and detect disconnection
	buffer := make([]byte, 4096)
	for {
		// Reading is just to detect disconnection, for now
		_, err := conn.Read(buffer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("Client disconnected:", conn.RemoteAddr())
			} else {
				fmt.Printf("Error reading from client %s:\n", conn.RemoteAddr())
			}
			// Client disconnected or error
			return
		}

		// If we want to implement ACK/NACK:
		// Parse command from buffer
		// if cmd == "ACK" { ... } else if cmd == "NACK" { ... }
	}
}
