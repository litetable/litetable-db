package engine

import (
	"db/internal/protocol"
	"fmt"
	"net"
)

// Engine is the main struct the provides the interface to the LiteTable server.
type Engine struct {
	maxBufferSize int
}

func New() (*Engine, error) {
	return &Engine{
		maxBufferSize: 4096,
	}, nil
}

// Handle implements the server.handler interface, allowing the engine to be used to respond
// to incoming TLS connections.
func (e *Engine) Handle(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Printf("Error closing connection: %v\n", err)
		}
	}()

	buf, err := e.read(conn)
	if err != nil {
		fmt.Printf("Read error: %v\n", err)
		return
	}

	msgType, queryBytes, decodeErr := protocol.Decode(buf)
	if decodeErr != nil {
		_, writeErr := conn.Write([]byte("ERROR: " + decodeErr.Error()))
		if writeErr != nil {
			fmt.Printf("Failed to write error: %v\n", writeErr)
		}
		return
	}

	// if query bytes are empty, return an error
	if len(queryBytes) == 0 {
		_, err = conn.Write([]byte("ERROR: Empty query"))
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
		}
		return
	}

	// before processing any query, write to the WAL

	// Always send a response for every operation type
	switch msgType {
	case protocol.Write:
		fmt.Println(string(queryBytes))
		_, err = conn.Write([]byte("WRITE_OK "))
	case protocol.Read:
		fmt.Println(string(queryBytes))
		_, err = conn.Write([]byte("READ_OK data "))
	case protocol.Delete:
		_, err = conn.Write([]byte("DELETE_OK "))
	case protocol.Unknown:
		_, err = conn.Write([]byte("ERROR: Unknown operation "))
	}

	// Check for write errors
	if err != nil {
		fmt.Printf("Error writing response: %v\n", err)
	}
}

// ever connection that is incoming must be read, create a buffer to read the connection
func (e *Engine) read(conn net.Conn) ([]byte, error) {
	buf := make([]byte, e.maxBufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
