package engine

import (
	"db/internal/protocol"
	"encoding/json"
	"fmt"
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

	// Always send a response for every operation type
	switch msgType {
	case protocol.Write:
		fmt.Println(string(queryBytes))
		got, err := e.write(queryBytes)
		if err != nil {
			_, err = conn.Write([]byte("ERROR: " + err.Error()))
			if err != nil {
				fmt.Printf("Error writing response: %v\n", err)
			}
			return
		}

		b, _ := json.Marshal(got)
		_, err = conn.Write(b)
	case protocol.Read:
		fmt.Println(string(queryBytes))
		got, err := e.Read(queryBytes)
		if err != nil {
			_, err = conn.Write([]byte("ERROR: " + err.Error()))
			if err != nil {
				fmt.Printf("Error writing response: %v\n", err)
			}
			return
		}
		b, _ := json.Marshal(got)
		_, err = conn.Write(b)
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
func (e *Engine) readConn(conn net.Conn) ([]byte, error) {
	buf := make([]byte, e.maxBufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
