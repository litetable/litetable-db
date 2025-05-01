package operations

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	wal2 "github.com/litetable/litetable-db/internal/wal"
	"time"
)

// Run accepts a buffer and decodes it into a message type and query bytes.
func (m *Manager) Run(buf []byte) ([]byte, error) {
	msgType, queryBytes := litetable.Decode(buf)
	// if query bytes are empty, return an error
	if len(queryBytes) == 0 {
		return nil, errEmptyQuery
	}

	// Only append to the WAL if this is not a READ or UNKNOWN
	if msgType != litetable.OperationRead && msgType != litetable.OperationUnknown {
		newEntry := &wal2.Entry{
			Operation: msgType,
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
	case litetable.OperationCreate:
		err := m.create(queryBytes)
		if err != nil {
			return nil, err
		}
		response = []byte("Family created successfully")
	case litetable.OperationWrite:
		result, writeErr := m.write(queryBytes)
		if writeErr != nil {
			return nil, writeErr
		}
		response = result
	case litetable.OperationRead:
		result, readErr := m.read(queryBytes)
		if readErr != nil {
			return nil, readErr
		}
		response = result
	case litetable.OperationDelete:
		deleteErr := m.delete(queryBytes)
		if deleteErr != nil {
			return nil, deleteErr
		}
		response = []byte("OK")
	case litetable.OperationUnknown:
		response = []byte("ERROR: Unknown operation ")
	}

	return response, nil
}
