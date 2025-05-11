package operations

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (m *Manager) Delete(query string) error {
	// Parse the query
	parsed, err := parseDeleteQuery(query)
	if err != nil {
		return err
	}

	err = m.shardStorage.Delete(parsed.rowKey, parsed.family, parsed.qualifiers, parsed.timestamp, parsed.expiresAt)
	if err != nil {
		return err
	}
	return nil
}

// delete marks data for garbage collection by placing tombstones in the column qualifier.
func (m *Manager) delete(query []byte) error {
	// Parse the query
	parsed, err := parseDeleteQuery(string(query))
	if err != nil {
		return err
	}

	err = m.shardStorage.Delete(parsed.rowKey, parsed.family, parsed.qualifiers, parsed.timestamp, parsed.expiresAt)
	if err != nil {
		return err
	}
	return nil
}

type deleteQuery struct {
	rowKey     string
	family     string
	qualifiers []string
	timestamp  int64 // this is either the current time or the provided timestamp
	ttl        int64
	expiresAt  int64
}

func parseDeleteQuery(input string) (*deleteQuery, error) {
	parts := strings.Fields(input)
	now := time.Now()
	defaultExpiresAt := now.Add(time.Hour).UnixNano()
	parsed := &deleteQuery{
		qualifiers: []string{},
		ttl:        3600,
		timestamp:  now.UnixNano(),
		expiresAt:  defaultExpiresAt,
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
		case "timestamp":
			timestamp, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp value: %s", value)
			}
			parsed.timestamp = timestamp
		case "ttl":
			ttlSec, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid ttl value: %s", value)
			}
			parsed.ttl = ttlSec

			ttlNanos := parsed.ttl * 1_000_000_000
			ttlTime := parsed.timestamp + ttlNanos
			parsed.expiresAt = ttlTime

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
