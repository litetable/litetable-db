package engine

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/protocol"
	"net"
)

// Handle implements the server.handler interface, allowing the engine to be used to respond
// to incoming TLS connections.
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
	if err = e.wal.Apply(msgType, queryBytes); err != nil {
		fmt.Printf("Failed to apply entry: %v\n", err)
		_, err = conn.Write([]byte("ERROR: " + err.Error()))
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
		}
		return
	}

	var response []byte
	// Always send a response for every operation type
	switch msgType {
	case protocol.Write:
		got, err := e.write(queryBytes)
		if err != nil {
			_, err = conn.Write([]byte("ERROR: " + err.Error()))
			if err != nil {
				fmt.Printf("Error writing response: %v\n", err)
			}
			return
		}

		b, _ := json.Marshal(got)
		response = b
	case protocol.Read:
		got, err := e.Read(queryBytes)
		if err != nil {
			_, err = conn.Write([]byte("ERROR: " + err.Error()))
			if err != nil {
				fmt.Printf("Error writing response: %v\n", err)
			}
			return
		}
		b, _ := json.Marshal(got)
		response = b
	case protocol.Delete:
		err = e.delete(queryBytes)
		if err != nil {
			_, err = conn.Write([]byte("ERROR: " + err.Error()))
			if err != nil {
				fmt.Printf("Error writing response: %v\n", err)
			}
			return
		}
		response = []byte("OK")
	case protocol.Unknown:
		err = fmt.Errorf("unknown operation")
		response = []byte("ERROR: Unknown operation ")
	}

	// Check for any errors
	if err != nil {
		fmt.Printf("Error writing response: %v\n", err)
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
