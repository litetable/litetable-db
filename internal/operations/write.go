package operations

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/cdc_emitter"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/reaper"
	"github.com/rs/zerolog/log"
	"net/url"
	"strconv"
	"strings"
	"time"
)

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

		newRow := litetable.TimestampedValue{
			Value:     value,
			Timestamp: parsed.timestamp,
		}

		// if we have an expiration time, write the time for all qualifier-value pairs
		if parsed.expiresAt != nil {
			newRow.IsTombstone = true
			newRow.ExpiresAt = *parsed.expiresAt
		}

		(*data)[parsed.rowKey][parsed.family][qualifier] = append(
			(*data)[parsed.rowKey][parsed.family][qualifier], newRow,
		)

		// Emit CDC event with the writeQuery details.
		m.cdc.Emit(&cdc_emitter.CDCParams{
			Operation: litetable.OperationWrite,
			RowKey:    parsed.rowKey,
			Column:    newRow,
		})

		// we need to do a double != nil checks because we don't want to send for garbage
		// collection before the data is saved.
		if parsed.expiresAt != nil {
			log.Debug().Msg("calling reaper on write operation")
			m.garbageCollector.Reap(&reaper.ReapParams{
				RowKey:     parsed.rowKey,
				Family:     parsed.family,
				Qualifiers: parsed.qualifiers,
				Timestamp:  parsed.timestamp,
				ExpiresAt:  *parsed.expiresAt,
			})
		}
	}

	// mark the row as changed
	m.storage.MarkRowChanged(parsed.family, parsed.rowKey)

	// The data has been saved, now let's just return what's written
	// Create response with all written values
	result := &litetable.Row{
		Key:     parsed.rowKey,
		Columns: make(map[string]litetable.VersionedQualifier),
	}
	result.Columns[parsed.family] = make(litetable.VersionedQualifier)

	for i, qualifier := range parsed.qualifiers {
		timestampedValue := litetable.TimestampedValue{
			Value:     parsed.values[i],
			Timestamp: parsed.timestamp,
		}

		// Store result
		values := []litetable.TimestampedValue{timestampedValue}
		result.Columns[parsed.family][qualifier] = values

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
	timestamp  time.Time
	expiresAt  *time.Time
	// ttl is the time the row should no longer be relevant from the time written
	ttl *int64
}

// parseWriteQuery parses a write query string into a structured form
func parseWriteQuery(input string) (*writeQuery, error) {
	parts := strings.Fields(input)
	parsed := &writeQuery{
		qualifiers: []string{},
		values:     [][]byte{},
		timestamp:  time.Now(),
		expiresAt:  nil,
		ttl:        nil,
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
			parsed.ttl = &ttlSec
			// expires at should be the write time + ttl
			expireTime := parsed.timestamp.Add(time.Duration(ttlSec) * time.Second)
			parsed.expiresAt = &expireTime
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
