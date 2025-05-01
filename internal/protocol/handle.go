package protocol

import (
	"fmt"
	wal2 "github.com/litetable/litetable-db/internal/wal"
	"time"
)

func (m *Manager) RunOperation(buf []byte) ([]byte, error) {
	msgType, queryBytes, decodeErr := Decode(buf)
	if decodeErr != nil {
		return nil, decodeErr
	}

	// if query bytes are empty, return an error
	if len(queryBytes) == 0 {
		return nil, errEmptyQuery
	}

	// Only append to the WAL if this is not a READ
	if msgType != Read {
		newEntry := &wal2.Entry{
			Protocol:  msgType,
			Query:     queryBytes,
			Timestamp: time.Now(),
		}
		if err := m.writeAhead.Apply(newEntry); err != nil {
			fmt.Printf("Failed to apply entry: %v\n", err)
			return nil, err
		}
	}

	var response []byte

	switch msgType {
	case Create:
		m.rwMutex.Lock()
		err := m.create(queryBytes)
		m.rwMutex.Unlock()
		if err != nil {
			return nil, err
		}
		response = []byte("Family created successfully")
	case Write:
		m.rwMutex.Lock()
		result, writeErr := m.Write(queryBytes)
		m.rwMutex.Unlock()
		if writeErr != nil {
			return nil, writeErr
		}
		response = result
	case Read:
		result, readErr := m.read(queryBytes)
		if readErr != nil {
			return nil, readErr
		}
		response = result
	case Delete:
		m.rwMutex.Lock()
		deleteErr := m.delete(queryBytes)
		m.rwMutex.Unlock()
		if deleteErr != nil {
			return nil, deleteErr
		}
		response = []byte("OK")
	case Unknown:
		response = []byte("ERROR: Unknown operation ")
	}

	return response, nil
}
