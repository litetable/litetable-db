package engine

import (
	"fmt"
	"net"
)

// Handle implements the server.handler interface, allowing the engine to be used to respond
// to incoming TLS connections. Handlers take the connection but are not allowed to return an error.
// Any errors should be written to the connection as to not block or crash the server.
func (e *Engine) Handle(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Printf("Error closing connection: %v\n", err)
		}
	}()

	buf, err := e.readConn(conn)
	if err != nil {
		fmt.Printf("Read error: %v\n", err)
		return
	}

	var response []byte
	res, err := e.protocol.RunOperation(buf)
	if err != nil {
		fmt.Printf("Error handling request: %v\n", err)
		response = []byte(fmt.Sprintf("Error: %v", err))
	} else {
		response = res
	}

	// Write the response to the connection
	_, err = conn.Write(response)
	if err != nil {
		fmt.Printf("Error writing response: %v\n", err)
		return
	}
}

// ever connection that is incoming must be read, create a buffer to read the connection
func (e *Engine) readConn(conn net.Conn) ([]byte, error) {
	buf := make([]byte, e.maxBufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
