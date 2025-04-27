package engine

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/protocol"
)

// Read processes a read query and returns the requested data.
// Supports all operations allowed by the Litetable protocol.
func (e *Engine) Read(query []byte) (interface{}, error) {
	// Parse the query
	parsed, err := protocol.ParseRead(string(query))
	if err != nil {
		return nil, err
	}

	if !e.isFamilyAllowed(parsed.Family) {
		return nil, fmt.Errorf("column family does not exist: %s", parsed.Family)
	}

	// Lock for reading
	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()

	// Case 1: Direct row key lookup
	if parsed.RowKey != "" {
		return parsed.ReadRowKey(e.Data())
	}

	// Case 2: Row key prefix filtering
	if parsed.RowKeyPrefix != "" {
		return parsed.FilterRowsByPrefix(e.Data())
	}

	// Case 3: Row key regex matching
	if parsed.RowKeyRegex != "" {
		return parsed.ReadRowsByRegex(e.Data())
	}

	return nil, fmt.Errorf("must provide rowKey, rowKeyPrefix, or rowKeyRegex")
}
