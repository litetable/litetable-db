package operations

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"net/url"
	"strings"
	"time"
)

// Write processes a mutation to update the data store
func (m *Manager) Write(query []byte) ([]byte, error) {
	// Parse the query
	parsed, err := parseWriteQuery(string(query))
	if err != nil {
		return nil, err
	}

	// Validate the family is allowed
	if !m.storage.IsFamilyAllowed(parsed.family) {
		return nil, fmt.Errorf("column family not allowed: %s", parsed.family)
	}

	data := m.storage.GetData()

	// Ensure the row exists
	if _, exists := (*data)[parsed.rowKey]; !exists {
		(*data)[parsed.rowKey] = make(map[string]litetable.VersionedQualifier)
	}

	// Ensure the family exists
	if _, exists := (*data)[parsed.rowKey][parsed.family]; !exists {
		(*data)[parsed.rowKey][parsed.family] = make(litetable.VersionedQualifier)
	}

	// Write all qualifier-value pairs with the same timestamp
	for i, qualifier := range parsed.qualifiers {
		value := parsed.values[i]
		(*data)[parsed.rowKey][parsed.family][qualifier] = append(
			(*data)[parsed.rowKey][parsed.family][qualifier],
			litetable.TimestampedValue{
				Value:     value,
				Timestamp: parsed.timestamp,
			},
		)
	}

	// Create response with all written values
	result := &litetable.Row{
		Key:     parsed.rowKey,
		Columns: make(map[string]litetable.VersionedQualifier),
	}
	result.Columns[parsed.family] = make(litetable.VersionedQualifier)

	for i, qualifier := range parsed.qualifiers {
		result.Columns[parsed.family][qualifier] = []litetable.TimestampedValue{
			{
				Value:     parsed.values[i],
				Timestamp: parsed.timestamp,
			},
		}
	}

	return json.Marshal(result)
}

type writeQuery struct {
	rowKey     string
	family     string
	qualifiers []string
	values     [][]byte
	timestamp  time.Time
}

// parseWriteQuery parses a write query string into a structured form
func parseWriteQuery(input string) (*writeQuery, error) {
	parts := strings.Fields(input)
	parsed := &writeQuery{
		qualifiers: []string{},
		values:     [][]byte{},
		timestamp:  time.Now(),
	}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid format: %s", part)
		}

		key, value := kv[0], kv[1]
		key = strings.TrimLeft(key, "-")

		// Decode URL-encoded values
		decodedValue, err := url.QueryUnescape(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value: %s", err)
		}

		switch key {
		case "key":
			parsed.rowKey = decodedValue
		case "family":
			parsed.family = decodedValue
		case "qualifier":
			parsed.qualifiers = append(parsed.qualifiers, decodedValue)
		case "value":
			parsed.values = append(parsed.values, []byte(decodedValue))
		case "timestamp":
			t, err := time.Parse(time.RFC3339, decodedValue)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp format: %s", decodedValue)
			}
			parsed.timestamp = t
		}
	}

	// Validation checks remain the same
	if parsed.rowKey == "" {
		return nil, fmt.Errorf("missing key")
	}
	if parsed.family == "" {
		return nil, fmt.Errorf("missing family")
	}
	if len(parsed.qualifiers) == 0 {
		return nil, fmt.Errorf("missing qualifier")
	}
	if len(parsed.values) == 0 {
		return nil, fmt.Errorf("missing value")
	}
	if len(parsed.qualifiers) != len(parsed.values) {
		return nil, fmt.Errorf("number of qualifiers (%d) doesn't match number of values (%d)",
			len(parsed.qualifiers), len(parsed.values))
	}

	return parsed, nil
}
