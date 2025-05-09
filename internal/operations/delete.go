package operations

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

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
	timestamp  time.Time // this is either the current time or the provided timestamp
	ttl        time.Duration
	expiresAt  *time.Time
}

func parseDeleteQuery(input string) (*deleteQuery, error) {
	parts := strings.Fields(input)
	now := time.Now()
	defaultExpiresAt := now.Add(time.Hour)
	parsed := &deleteQuery{
		qualifiers: []string{},
		ttl:        time.Duration(3600) * time.Second,
		timestamp:  now,
		expiresAt:  &defaultExpiresAt,
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
			t, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp format: %s", value)
			}
			parsed.timestamp = t.Add(time.Nanosecond) // make a slight increment in the time
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

	// if TTL was provided, calculate the expiresAt time based on timestamp
	if parsed.ttl > 0 {
		ttlTime := parsed.timestamp.Add(parsed.ttl)
		parsed.expiresAt = &ttlTime
	}

	// Validate required fields
	if parsed.rowKey == "" {
		return nil, fmt.Errorf("missing key")
	}

	return parsed, nil
}
