package engine

import (
	"db/internal/litetable"
	"fmt"
	"strings"
	"time"
)

// Write processes a mutation to update the data store
func (e *Engine) write(query []byte) (*writeQuery, error) {
	// Parse the query
	parsed, err := parseWriteQuery(string(query))
	if err != nil {
		return nil, err
	}

	// Lock for writing
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	// Ensure the row exists
	if _, exists := e.data[parsed.RowKey]; !exists {
		e.data[parsed.RowKey] = make(map[string]litetable.VersionedQualifier)
	}

	// Ensure the family exists
	if _, exists := e.data[parsed.RowKey][parsed.Family]; !exists {
		e.data[parsed.RowKey][parsed.Family] = make(litetable.VersionedQualifier)
	}

	// Write the value
	e.data[parsed.RowKey][parsed.Family][parsed.Qualifier] = append(
		e.data[parsed.RowKey][parsed.Family][parsed.Qualifier],
		litetable.TimestampedValue{
			Value:     parsed.Value,
			Timestamp: parsed.Timestamp,
		},
	)

	return parsed, nil
}

type writeQuery struct {
	RowKey    string    `json:"key"`
	Family    string    `json:"family"`
	Qualifier string    `json:"qualifier"`
	Value     []byte    `json:"value"`
	Timestamp time.Time `json:"timestamp"` // Parsed timestamp
}

// parseWriteQuery parses a write query string into a structured form
func parseWriteQuery(input string) (*writeQuery, error) {
	parts := strings.Fields(input)
	parsed := &writeQuery{
		Timestamp: time.Now(), // Default to current time if not specified
	}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid format: %s", part)
		}

		key, value := kv[0], kv[1]

		switch key {
		case "key":
			parsed.RowKey = value
		case "family":
			parsed.Family = value
		case "qualifier":
			parsed.Qualifier = value
		case "value":
			parsed.Value = []byte(value)
		case "timestamp":
			t, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp format: %s", value)
			}
			parsed.Timestamp = t
		}
	}

	// Validate required fields
	if parsed.RowKey == "" {
		return nil, fmt.Errorf("missing key")
	}
	if parsed.Family == "" {
		return nil, fmt.Errorf("missing family")
	}
	if parsed.Qualifier == "" {
		return nil, fmt.Errorf("missing qualifier")
	}
	if parsed.Value == nil {
		return nil, fmt.Errorf("missing value")
	}

	return parsed, nil
}
