package operations

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (m *Manager) Write(query string) (map[string]*litetable.Row, error) {
	// Parse the query
	parsed, err := parseWriteQuery(query)
	if err != nil {
		return nil, err
	}

	// Use the shard_storage Apply method to write data
	err = m.shardStorage.Apply(
		parsed.rowKey,
		parsed.family,
		parsed.qualifiers,
		parsed.values,
		parsed.timestamp,
		parsed.expiresAt,
	)
	if err != nil {
		return nil, err
	}

	// The data has been saved, now let's just return what's written
	// Create response with all written values
	row := &litetable.Row{
		Key:     parsed.rowKey,
		Columns: make(map[string]litetable.VersionedQualifier),
	}
	row.Columns[parsed.family] = make(litetable.VersionedQualifier)

	for i, qualifier := range parsed.qualifiers {
		timestampedValue := litetable.TimestampedValue{
			Value:     parsed.values[i],
			Timestamp: parsed.timestamp,
		}

		// Store result
		values := []litetable.TimestampedValue{timestampedValue}
		row.Columns[parsed.family][qualifier] = values

	}

	result := map[string]*litetable.Row{
		row.Key: row,
	}

	return result, nil
}

// Write processes a mutation to update the data store; this is an append-only operation
//
// When writing a row, the following condition must be true:
// 1. The family must be allowed in the table
//
// When writing to a row, an expiration time can be set for the row; this is called a tombstone.
// Any data written before the tombstone will also be garbage collected.
func (m *Manager) write(query []byte) ([]byte, error) {
	// Parse the query
	parsed, err := parseWriteQuery(string(query))
	if err != nil {
		return nil, err
	}

	// Use the shard_storage Apply method to write data
	err = m.shardStorage.Apply(
		parsed.rowKey,
		parsed.family,
		parsed.qualifiers,
		parsed.values,
		parsed.timestamp,
		parsed.expiresAt,
	)
	if err != nil {
		return nil, err
	}

	// The data has been saved, now let's just return what's written
	// Create response with all written values
	row := &litetable.Row{
		Key:     parsed.rowKey,
		Columns: make(map[string]litetable.VersionedQualifier),
	}
	row.Columns[parsed.family] = make(litetable.VersionedQualifier)

	for i, qualifier := range parsed.qualifiers {
		timestampedValue := litetable.TimestampedValue{
			Value:     parsed.values[i],
			Timestamp: parsed.timestamp,
		}

		// Store result
		values := []litetable.TimestampedValue{timestampedValue}
		row.Columns[parsed.family][qualifier] = values

	}

	result := map[string]*litetable.Row{
		row.Key: row,
	}

	return json.Marshal(result)
}

// writeQuery are the possible values to be passed in the query that manipulate the write
// behavior to the table.
//
// Note: ttl is globally applied to all rows in the write.
type writeQuery struct {
	rowKey     string
	family     string
	qualifiers []string
	values     [][]byte
	timestamp  int64
	expiresAt  int64
	// ttl is the time the row should no longer be relevant from the time written
	ttl int64
}

// parseWriteQuery parses a write query string into a structured form
func parseWriteQuery(input string) (*writeQuery, error) {
	parts := strings.Fields(input)
	parsed := &writeQuery{
		qualifiers: []string{},
		values:     [][]byte{},
		timestamp:  time.Now().UnixNano(),
		expiresAt:  0,
		ttl:        0,
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
		case "ttl":
			ttlSec, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid ttl value: %s", value)
			}
			parsed.ttl = ttlSec
			// expires at should be the write time + ttl
			expiresAtTimestamp := parsed.timestamp + ttlSec
			parsed.expiresAt = expiresAtTimestamp
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
