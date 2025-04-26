package engine

import (
	"net"
)

// Engine is the main struct the provides the interface to the LiteTable server.
type Engine struct{}

func New() (*Engine, error) {
	return &Engine{}, nil
}

// Handle implements the server.handler interface, allowing the engine to be used to respond
// to incoming TLS connections.
func (e *Engine) Handle(conn net.Conn) {}
