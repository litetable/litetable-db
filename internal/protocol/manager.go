package protocol

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
)

type dataStorage interface {
	Write(rowKey, family string, qualifier litetable.VersionedQualifier) error
}

type Manager struct {
	dataStorage dataStorage
}

type Config struct {
	Storage dataStorage
}

func (c *Config) validate() error {
	if c.Storage == nil {
		return fmt.Errorf("storage is required")
	}
	return nil
}

// New creates a new protocol manager
func New(cfg *Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Manager{
		dataStorage: cfg.Storage,
	}, nil
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
