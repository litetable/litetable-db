package protocol

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// readQuery are the parameters for any supported read query
type readQuery struct {
	RowKey       string
	RowKeyPrefix string
	RowKeyRegex  string
	Family       string
	Qualifiers   []string
	Latest       int       // Number of most recent versions to return
	Timestamp    time.Time // Reserved for future use
}

// parseRead parses a query and returns a ReadQuery which is used to safely run an operation.
// If there are any errors, it will return a protocol.Error
func parseRead(input string) (*readQuery, error) {
	parts := strings.Fields(input)
	parsed := &readQuery{
		Qualifiers: []string{},
		Latest:     0, // 0 means all versions
	}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, newError(ErrInvalidFormat,
				"queries must include at least a column family and a search key, got: %s",
				input)
		}

		key, value := kv[0], kv[1]

		switch key {
		case "key":
			parsed.RowKey = value
		case "prefix":
			parsed.RowKeyPrefix = value
		case "regex":
			parsed.RowKeyRegex = value
		case "family":
			parsed.Family = value
		case "qualifier":
			parsed.Qualifiers = append(parsed.Qualifiers, value)
		case "latest":
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, newError(ErrInvalidFormat, "latest must be a number. received %s",
					value)
			}
			if n < 0 {
				return nil, newError(ErrInvalidFormat,
					"latest must be greater than 0. received %d", n)
			}
			parsed.Latest = n
		case "timestamp":
			t, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, newError(ErrInvalidFormat, "invalid timestamp format: %s", value)
			}
			parsed.Timestamp = t
		default:
			return nil, newError(ErrUnknownParameter, "%s", key)
		}
	}

	// Validate that at least one search key is provided
	if parsed.RowKey == "" && parsed.RowKeyPrefix == "" && parsed.RowKeyRegex == "" {
		return nil, newError(ErrMissingKey, "missing search key: provide one of key, prefix, or regex")
	}

	// Validate that exactly one search key type is provided
	keyCount := 0
	if parsed.RowKey != "" {
		keyCount++
	}
	if parsed.RowKeyPrefix != "" {
		keyCount++
	}
	if parsed.RowKeyRegex != "" {
		keyCount++
	}

	if keyCount > 1 {
		return nil, newError(ErrInvalidFormat,
			"only one search key type allowed: provide exactly one of rowKey, rowKeyPrefix, "+
				"or rowKeyRegex")
	}

	// Family is always required
	if parsed.Family == "" {
		return nil, newError(ErrInvalidFormat, "missing family")
	}

	return parsed, nil
}

// readRowKey reads a single row by its key and returns the requested data. If the latest filter
// is provided in the query, it will return only the latest N versions of the qualifiers.
func (r *readQuery) readRowKey(data *DataFormat) (*litetable.Row, error) {
	// Check if the row exists
	row, exists := (*data)[r.RowKey]
	if !exists {
		return nil, fmt.Errorf("row not found: %s", r.RowKey)
	}

	// Check if the family exists
	family, exists := row[r.Family]
	if !exists {
		return nil, fmt.Errorf("family not found: %s", r.Family)
	}

	// Create result container
	result := &litetable.Row{
		Key:     r.RowKey,
		Columns: make(map[string]litetable.VersionedQualifier),
	}
	result.Columns[r.Family] = make(litetable.VersionedQualifier)

	// If no qualifiers specified, return all qualifiers in the family
	if len(r.Qualifiers) == 0 {
		// Copy all qualifiers and their values
		for qualifier, values := range family {
			result.Columns[r.Family][qualifier] = r.getLatestN(values, r.Latest)
		}
	} else {
		// Return only requested qualifiers
		for _, qualifier := range r.Qualifiers {
			values, exists := family[qualifier]
			if !exists {
				continue // Skip non-existing qualifiers
			}
			result.Columns[r.Family][qualifier] = r.getLatestN(values, r.Latest)
		}
	}

	return result, nil
}

// filterRowsByPrefix reads from all rows with the specified prefix and returns the requested data.
// If the latest filter is provided in the query, it will return only the latest N versions of all
// qualifiers in the family.
func (r *readQuery) filterRowsByPrefix(data *DataFormat) (map[string]*litetable.Row, error) {
	results := make(map[string]*litetable.Row)

	for rowKey, rowData := range *data {
		prefixMatch := strings.HasPrefix(rowKey, r.RowKeyPrefix)
		if prefixMatch {
			// Skip rows that don't have the requested family
			family, exists := rowData[r.Family]
			if !exists {
				continue
			}

			// Create result container for this row
			result := &litetable.Row{
				Key:     rowKey,
				Columns: make(map[string]litetable.VersionedQualifier),
			}
			result.Columns[r.Family] = make(litetable.VersionedQualifier)

			// If no qualifiers specified, return all qualifiers in the family
			if len(r.Qualifiers) == 0 {
				for qualifier, values := range family {
					result.Columns[r.Family][qualifier] = r.getLatestN(values, r.Latest)
				}
			} else {
				// Return only requested qualifiers
				for _, qualifier := range r.Qualifiers {
					values, exists := family[qualifier]
					if !exists {
						continue // Skip non-existing qualifiers
					}
					result.Columns[r.Family][qualifier] = r.getLatestN(values, r.Latest)
				}
			}

			results[rowKey] = result
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no rows found with prefix: %s", r.RowKeyPrefix)
	}

	return results, nil
}

// readRowsByRegex reads from all rows matching the specified regex and returns the requested data.
// If the latest filter is provided in the query, it will return only the latest N versions of all
// qualifiers in the family.
func (r *readQuery) readRowsByRegex(data *DataFormat) (map[string]*litetable.Row, error) {

	regex, err := regexp.Compile(r.RowKeyRegex)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	results := make(map[string]*litetable.Row)

	for rowKey, rowData := range *data {
		if regex.MatchString(rowKey) {
			// Skip rows that don't have the requested family
			family, exists := rowData[r.Family]
			if !exists {
				continue
			}

			// Create result container for this row
			result := &litetable.Row{
				Key:     rowKey,
				Columns: make(map[string]litetable.VersionedQualifier),
			}
			result.Columns[r.Family] = make(litetable.VersionedQualifier)

			// If no qualifiers specified, return all qualifiers in the family
			if len(r.Qualifiers) == 0 {
				for qualifier, values := range family {
					result.Columns[r.Family][qualifier] = r.getLatestN(values, r.Latest)
				}
			} else {
				// Return only requested qualifiers
				for _, qualifier := range r.Qualifiers {
					values, exists := family[qualifier]
					if !exists {
						continue // Skip non-existing qualifiers
					}
					result.Columns[r.Family][qualifier] = r.getLatestN(values, r.Latest)
				}
			}

			results[rowKey] = result
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no rows found matching regex: %s", r.RowKeyRegex)
	}

	return results, nil
}

// getLatestN returns the latest N values from a slice of TimestampedValue.
func (r *readQuery) getLatestN(values []litetable.TimestampedValue,
	n int) []litetable.TimestampedValue {
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
