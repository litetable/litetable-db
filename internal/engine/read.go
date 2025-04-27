package engine

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Read processes a read query and returns the requested data.
// Supports direct key lookup, prefix filtering, and regex matching.
func (e *Engine) Read(query []byte) (interface{}, error) {
	// Parse the query
	parsed, err := parseReadQuery(string(query))
	if err != nil {
		return nil, err
	}

	if !e.isFamilyAllowed(parsed.family) {
		return nil, fmt.Errorf("column family does not exist: %s", parsed.family)
	}

	// Lock for reading
	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()

	// Case 1: Direct row key lookup
	if parsed.rowKey != "" {
		return e.readSingleRow(parsed)
	}

	// Case 2: Row key prefix filtering
	if parsed.rowKeyPrefix != "" {
		return e.readRowsByPrefix(parsed)
	}

	// Case 3: Row key regex matching
	if parsed.rowKeyRegex != "" {
		return e.readRowsByRegex(parsed)
	}

	return nil, fmt.Errorf("must provide rowKey, rowKeyPrefix, or rowKeyRegex")
}

// readSingleRow reads a single row based on the exact key
func (e *Engine) readSingleRow(parsed *readQuery) (*litetable.Row, error) {
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
			result.Columns[parsed.family][qualifier] = getLatestN(values, parsed.latest)
		}
	} else {
		// Return only requested qualifiers
		for _, qualifier := range parsed.qualifiers {
			values, exists := family[qualifier]
			if !exists {
				continue // Skip non-existing qualifiers
			}
			result.Columns[parsed.family][qualifier] = getLatestN(values, parsed.latest)
		}
	}

	return result, nil
}

// readRowsByPrefix returns all rows that start with the given prefix
func (e *Engine) readRowsByPrefix(parsed *readQuery) (map[string]*litetable.Row, error) {
	results := make(map[string]*litetable.Row)

	for rowKey, rowData := range e.data {
		prefixMatch := strings.HasPrefix(rowKey, parsed.rowKeyPrefix)
		if prefixMatch {
			// Skip rows that don't have the requested family
			family, exists := rowData[parsed.family]
			if !exists {
				continue
			}

			// Create result container for this row
			result := &litetable.Row{
				Key:     rowKey,
				Columns: make(map[string]litetable.VersionedQualifier),
			}
			result.Columns[parsed.family] = make(litetable.VersionedQualifier)

			// If no qualifiers specified, return all qualifiers in the family
			if len(parsed.qualifiers) == 0 {
				for qualifier, values := range family {
					result.Columns[parsed.family][qualifier] = getLatestN(values, parsed.latest)
				}
			} else {
				// Return only requested qualifiers
				for _, qualifier := range parsed.qualifiers {
					values, exists := family[qualifier]
					if !exists {
						continue // Skip non-existing qualifiers
					}
					result.Columns[parsed.family][qualifier] = getLatestN(values, parsed.latest)
				}
			}

			results[rowKey] = result
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no rows found with prefix: %s", parsed.rowKeyPrefix)
	}

	return results, nil
}

// readRowsByRegex returns all rows that match the given regex pattern
func (e *Engine) readRowsByRegex(parsed *readQuery) (map[string]*litetable.Row, error) {

	regex, err := regexp.Compile(parsed.rowKeyRegex)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	results := make(map[string]*litetable.Row)

	for rowKey, rowData := range e.data {
		if regex.MatchString(rowKey) {
			// Skip rows that don't have the requested family
			family, exists := rowData[parsed.family]
			if !exists {
				continue
			}

			// Create result container for this row
			result := &litetable.Row{
				Key:     rowKey,
				Columns: make(map[string]litetable.VersionedQualifier),
			}
			result.Columns[parsed.family] = make(litetable.VersionedQualifier)

			// If no qualifiers specified, return all qualifiers in the family
			if len(parsed.qualifiers) == 0 {
				for qualifier, values := range family {
					result.Columns[parsed.family][qualifier] = getLatestN(values, parsed.latest)
				}
			} else {
				// Return only requested qualifiers
				for _, qualifier := range parsed.qualifiers {
					values, exists := family[qualifier]
					if !exists {
						continue // Skip non-existing qualifiers
					}
					result.Columns[parsed.family][qualifier] = getLatestN(values, parsed.latest)
				}
			}

			results[rowKey] = result
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no rows found matching regex: %s", parsed.rowKeyRegex)
	}

	return results, nil
}

type readQuery struct {
	rowKey       string
	rowKeyPrefix string
	rowKeyRegex  string
	family       string
	qualifiers   []string
	latest       int       // Number of most recent versions to return
	timestamp    time.Time // Reserved for future use
}

// Update the parseReadQuery function to better debug prefix issues
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
		case "prefix":
			parsed.rowKeyPrefix = value
		case "regex":
			parsed.rowKeyRegex = value
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
		default:
			return nil, fmt.Errorf("unknown parameter: %s", key)
		}
	}

	// Validate that at least one search key is provided
	if parsed.rowKey == "" && parsed.rowKeyPrefix == "" && parsed.rowKeyRegex == "" {
		return nil, fmt.Errorf("missing search key: provide one of key, prefix, or regex")
	}

	// If multiple search keys are provided, prioritize in order: rowKey, prefix, regex
	if parsed.rowKey != "" {
		parsed.rowKeyPrefix = ""
		parsed.rowKeyRegex = ""
	} else if parsed.rowKeyPrefix != "" {
		parsed.rowKeyRegex = ""
	}

	// Family is always required
	if parsed.family == "" {
		return nil, fmt.Errorf("missing family")
	}

	return parsed, nil
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

	// Otherwise, return the top n values
	return valuesCopy[:n]
}
