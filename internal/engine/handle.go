package engine

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"net"
)

// Handle implements the server.handler interface, allowing the engine to be used to respond
// to incoming TLS connections. Handlers take the connection but are not allowed to return an error.
// Any errors should be written to the connection as to not block or crash the server.
func (e *Engine) Handle(conn Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing connection")
		}
	}()

	buf, err := e.readConn(conn)
	if err != nil {
		// Write the error to the connection
		_, writeErr := conn.Write([]byte(fmt.Sprintf("Error: %v", err)))
		if writeErr != nil {
			log.Error().Err(writeErr).Msg("Error writing response")
		}
		return
	}

	var response []byte
	res, err := e.operations.Run(buf)
	if err != nil {
		log.Error().Err(err).Msg("Error handling request")
		response = []byte(fmt.Sprintf("Error: %v", err))
	} else {
		response = res
	}

	// Write the response to the connection
	_, err = conn.Write(response)
	if err != nil {
		log.Error().Err(err).Msg("Error writing response")
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
