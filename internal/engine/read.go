package engine

import (
	"db/internal/litetable"
	"db/internal/protocol"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Read processes a read query and returns the requested data.
//
// Input example: key=testKey:12345 family=main qualifier=status qualifier=time latest=2
//
// The return value would be a read of all data for that row key and family,
// or if qualifiers are specified, the latest N versions of those qualifiers
func (e *Engine) Read(query []byte) (*litetable.Row, error) {
	// Log the read operation
	if err := e.wal.Apply(protocol.Read, query); err != nil {
		return nil, fmt.Errorf("failed to log read operation: %w", err)
	}

	// Parse the query
	parsed, err := parseReadQuery(string(query))
	if err != nil {
		return nil, err
	}

	// Check if the row exists
	row, exists := e.data[parsed.rowKey]
	if !exists {
		return nil, fmt.Errorf("row not found: %s", parsed.rowKey)
	}

	// Check if the family exists
	family, exists := row[parsed.family]
	if !exists {
		return nil, fmt.Errorf("family not found: %s", parsed.family)
	}

	// Create result container
	result := &litetable.Row{
		Key:     parsed.rowKey,
		Columns: make(map[string]litetable.VersionedQualifier),
	}
	result.Columns[parsed.family] = make(litetable.VersionedQualifier)

	// If no qualifiers specified, return all qualifiers in the family
	if len(parsed.qualifiers) == 0 {
		// Copy all qualifiers and their values
		for qualifier, values := range family {
			// Always use getLatestN - when parsed.latest is 0, it will return all values sorted
			result.Columns[parsed.family][qualifier] = getLatestN(values, parsed.latest)
		}
	} else {
		// Return only requested qualifiers
		for _, qualifier := range parsed.qualifiers {
			values, exists := family[qualifier]
			if !exists {
				continue // Skip non-existing qualifiers
			}

			// Always use getLatestN - when parsed.latest is 0, it will return all values sorted
			result.Columns[parsed.family][qualifier] = getLatestN(values, parsed.latest)
		}
	}

	return result, nil
}

// getLatestN returns up to n most recent timestamped values
// If n is 0, returns all values sorted by timestamp descending
func getLatestN(values []litetable.TimestampedValue, n int) []litetable.TimestampedValue {
	// Create a copy to avoid modifying the original
	valuesCopy := make([]litetable.TimestampedValue, len(values))
	copy(valuesCopy, values)

	// Sort by timestamp descending (newest first)
	sort.Slice(valuesCopy, func(i, j int) bool {
		return valuesCopy[i].Timestamp.After(valuesCopy[j].Timestamp)
	})

	// If n is 0 or greater than the length, return all values
	if n <= 0 || n >= len(valuesCopy) {
		return valuesCopy
	}

	// Otherwise return the top n values
	return valuesCopy[:n]
}

type readQuery struct {
	rowKey     string
	family     string
	qualifiers []string
	latest     int       // Number of most recent versions to return
	timestamp  time.Time // Reserved for future use
}

// parseReadQuery parses a read query string into a structured form
func parseReadQuery(input string) (*readQuery, error) {
	parts := strings.Fields(input)
	parsed := &readQuery{
		qualifiers: []string{},
		latest:     0, // 0 means all versions
	}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid format: %s", part)
		}

		key, value := kv[0], kv[1]

		switch key {
		case "key":
			parsed.rowKey = value
		case "family":
			parsed.family = value
		case "qualifier":
			parsed.qualifiers = append(parsed.qualifiers, value)
		case "latest":
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid latest value, must be a number: %s", value)
			}
			if n < 0 {
				return nil, fmt.Errorf("latest value cannot be negative: %d", n)
			}
			parsed.latest = n
		case "timestamp":
			t, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp format: %s", value)
			}
			parsed.timestamp = t
		}
	}

	// Validate required fields
	if parsed.rowKey == "" {
		return nil, fmt.Errorf("missing key")
	}
	if parsed.family == "" {
		return nil, fmt.Errorf("missing family")
	}

	return parsed, nil
}
