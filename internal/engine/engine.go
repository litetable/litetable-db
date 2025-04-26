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
			// Handle error
			return
		}
	}()

	buf, err := e.read(conn)
	if err != nil {
		// Handle error
		return
	}

	/*
		Over the buffer, we have to decode and handle our message, which must adhere to the
		protocol defined in the protocol package.
	*/
	decodedMsg, decodeErr := protocol.Decode(buf)
	if decodeErr != nil {
		// send an error over the connection
		_, writeErr := conn.Write([]byte("ERROR: " + decodeErr.Error()))
		if writeErr != nil {
			_ = fmt.Errorf("failed to write error: %v", writeErr)
		}
		return
	}

	// depending on the message type, call the appropriate function
	switch decodedMsg {
	case protocol.Write:
		// Handle write operation
		conn.Write([]byte("Write operation handled"))
	case protocol.Read:
		// Handle read operation
		fmt.Println("Handling read operation")
	case protocol.Delete:
		// Handle delete operation
		fmt.Println("Handling delete operation")
	}

	fmt.Println("Received message type:", decodedMsg)
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
