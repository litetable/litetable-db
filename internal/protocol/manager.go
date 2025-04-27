package protocol

import (
	"encoding/json"
	"fmt"
)

type protocol interface {
	Read(params *ReadParams) ([]byte, error)
	Write(data []byte) ([]byte, error)
	Delete() error
}
type Manager struct{}

func New() *Manager {
	return &Manager{}
}

type ReadParams struct {
	Query              []byte
	Data               *DataFormat
	ConfiguredFamilies []string
}

// Read applies a read query over a datasource following the Litetable protocol.
func (m *Manager) Read(params *ReadParams) ([]byte, error) {
	// Parse the query
	parsed, err := parseRead(string(params.Query))
	if err != nil {
		return nil, err
	}

	if !isFamilyAllowed(params.ConfiguredFamilies, parsed.Family) {
		return nil, fmt.Errorf("column family does not exist: %s", parsed.Family)
	}

	// Case 1: Direct row key lookup
	if parsed.RowKey != "" {
		result, readRowErr := parsed.readRowKey(params.Data)
		if readRowErr != nil {
			return nil, readRowErr
		}
		return json.Marshal(result)
	}

	// Case 2: Row key prefix filtering
	if parsed.RowKeyPrefix != "" {
		result, filterRowsErr := parsed.filterRowsByPrefix(params.Data)
		if filterRowsErr != nil {
			return nil, filterRowsErr
		}
		return json.Marshal(result)
	}

	// Case 3: Row key regex matching
	if parsed.RowKeyRegex != "" {
		result, readRowsErr := parsed.readRowsByRegex(params.Data)
		if readRowsErr != nil {
			return nil, readRowsErr
		}
		return json.Marshal(result)
	}

	return nil, newError(ErrInvalidFormat, "must provide rowKey, rowKeyPrefix, or rowKeyRegex")
}

func (m *Manager) Write(data []byte) ([]byte, error) {
	return nil, nil
}

func (m *Manager) Delete() error {
	return nil
}
