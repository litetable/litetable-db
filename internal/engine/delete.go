package engine

import (
	"db/internal/litetable"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// delete marks data for deletion in the store using tombstones
func (e *Engine) delete(query []byte) error {
	// Parse the query
	parsed, err := parseDeleteQuery(string(query))
	if err != nil {
		return err
	}

	// Lock for writing
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	// Check if the row exists
	row, exists := e.data[parsed.rowKey]
	if !exists {
		return fmt.Errorf("row not found: %s", parsed.rowKey)
	}

	// BigTable-like approach: Add tombstone markers
	now := time.Now()

	if parsed.family == "" {
		// Mark the entire row for deletion by adding tombstones to all families
		for familyName, family := range row {
			for qualifier := range family {
				addTombstone(row, familyName, qualifier, now, parsed.ttl)
			}
		}
	} else {
		family, exists := row[parsed.family]
		if !exists {
			return fmt.Errorf("family not found: %s", parsed.family)
		}

		if len(parsed.qualifiers) == 0 {
			// Mark entire family for deletion
			for qualifier := range family {
				addTombstone(row, parsed.family, qualifier, now, parsed.ttl)
			}
		} else {
			// Mark specific qualifiers
			for _, qualifier := range parsed.qualifiers {
				if _, exists := family[qualifier]; exists {
					addTombstone(row, parsed.family, qualifier, now, parsed.ttl)
				}
			}
		}
	}

	return nil
}

// addTombstone adds a tombstone marker for a cell
func addTombstone(row map[string]litetable.VersionedQualifier, family, qualifier string, timestamp time.Time, ttl time.Duration) {
	expiresAt := time.Time{} // Zero time means no automatic expiration
	if ttl > 0 {
		expiresAt = timestamp.Add(ttl)
	}

	row[family][qualifier] = append(row[family][qualifier], litetable.TimestampedValue{
		Value:       nil,
		Timestamp:   timestamp,
		IsTombstone: true,
		ExpiresAt:   expiresAt,
	})
}

type deleteQuery struct {
	rowKey     string
	family     string
	qualifiers []string
	ttl        time.Duration
}

func parseDeleteQuery(input string) (*deleteQuery, error) {
	parts := strings.Fields(input)
	parsed := &deleteQuery{
		qualifiers: []string{},
		ttl:        0, // Default to no automatic expiration
	}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid format: %s", part)
		}

		key, value := kv[0], kv[1]
		key = strings.TrimLeft(key, "-")

		switch key {
		case "key":
			parsed.rowKey = value
		case "family":
			parsed.family = value
		case "qualifier":
			parsed.qualifiers = append(parsed.qualifiers, value)
		case "ttl":
			ttlSec, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid ttl value: %s", value)
			}
			parsed.ttl = time.Duration(ttlSec) * time.Second
		default:
			return nil, fmt.Errorf("unknown parameter: %s", key)
		}
	}

	// Validate required fields
	if parsed.rowKey == "" {
		return nil, fmt.Errorf("missing key")
	}

	return parsed, nil
}
