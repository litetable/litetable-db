package engine

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/protocol"
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

	msgType, queryBytes, decodeErr := protocol.Decode(buf)
	if decodeErr != nil {
		_, writeErr := conn.Write([]byte(decodeErr.Error()))
		if writeErr != nil {
			fmt.Printf("Failed to write error: %v\n", writeErr)
		}
		return
	}

	// if query bytes are empty, return an error
	if len(queryBytes) == 0 {
		_, err = conn.Write([]byte("Empty query"))
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
		}
		return
	}

	// before processing any query, write to the WAL
	if err = e.wal.Apply(msgType, queryBytes); err != nil {
		fmt.Printf("Failed to apply entry: %v\n", err)
		_, err = conn.Write([]byte(err.Error()))
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
		}
		return
	}

	// Lock for writing
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	var response []byte

	switch msgType {
	case protocol.Create:
		err = e.protocol.Create(&protocol.CreateParams{
			Query:              queryBytes,
			ConfiguredFamilies: e.allowedFamilies,
			CB:                 e.saveAllowedFamilies,
		})
		if err != nil {
			response = []byte(err.Error())
		} else {
			response = []byte("Family created successfully")
		}

	case protocol.Write:
		result, writeErr := e.protocol.Write(&protocol.WriteParams{
			Query:              queryBytes,
			Data:               e.Data(),
			ConfiguredFamilies: e.allowedFamilies,
		})
		if writeErr != nil {
			response = []byte(writeErr.Error())
		} else {
			response = result
		}
	case protocol.Read:
		result, readErr := e.protocol.Read(&protocol.ReadParams{
			Query:              queryBytes,
			Data:               e.Data(),
			ConfiguredFamilies: e.allowedFamilies,
		})
		if readErr != nil {
			response = []byte(readErr.Error())
		} else {
			response = result
		}
	case protocol.Delete:
		deleteErr := e.protocol.Delete(&protocol.DeleteParams{
			Query:              queryBytes,
			Data:               e.Data(),
			ConfiguredFamilies: e.allowedFamilies,
		})
		if deleteErr != nil {
			response = []byte(deleteErr.Error())
		} else {
			response = []byte("OK")
		}
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
