package operations

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (m *Manager) Read(query string) (map[string]*litetable.Row, error) {
	// Parse the query
	parsed, err := parseRead(query)
	if err != nil {
		return nil, err
	}

	if !m.shardStorage.IsFamilyAllowed(parsed.family) {
		return nil, fmt.Errorf("column family does not exist: %s", parsed.family)
	}

	// Alt case 1: Row key prefix filtering
	if parsed.rowKeyPrefix != "" {
		d, found := m.shardStorage.FilterRowsByPrefix(parsed.rowKeyPrefix)
		if !found {
			return nil, fmt.Errorf("no rows found with prefix: %s", parsed.rowKeyPrefix)
		}

		result := parsed.processFilteredData(*d)
		if len(result) == 0 {
			return nil, fmt.Errorf("no matching rows found with prefix: %s", parsed.rowKeyPrefix)
		}
		return result, nil
	}

	// Alt case 2: Row key regex matching
	if parsed.rowKeyRegex != "" {
		data, found := m.shardStorage.FilterRowsByRegex(parsed.rowKeyRegex)
		if !found {
			return nil, fmt.Errorf("no rows found matching regex: %s", parsed.rowKeyRegex)
		}

		result := parsed.processFilteredData(*data)
		if len(result) == 0 {
			return nil, fmt.Errorf("no matching rows found with regex: %s", parsed.rowKeyRegex)

		}

		return result, nil
	}

	// default to read by rowKey:
	data, exists := m.shardStorage.GetRowByFamily(parsed.rowKey, parsed.family)
	if !exists {
		return nil, fmt.Errorf("row not found: %s", parsed.rowKey)
	}

	// Create a proper Row structure with the data
	row, err := parsed.readRowKey(data)
	if err != nil {
		return nil, err
	}

	r := map[string]*litetable.Row{
		row.Key: row,
	}

	return r, nil
}

// readQuery are the parameters for any supported read query
type readQuery struct {
	rowKey       string
	rowKeyPrefix string
	rowKeyRegex  string
	family       string
	qualifiers   []string
	latest       int       // Number of most recent versions to return
	timestamp    time.Time // Reserved for future use
}

// parseRead parses a query and returns a ReadQuery which is used to safely run an operation.
// If there are any errors, it will return a operations.Error
func parseRead(input string) (*readQuery, error) {
	parts := strings.Fields(input)
	parsed := &readQuery{
		qualifiers: []string{},
		latest:     0, // 0 means all versions
	}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, newError(errInvalidFormat,
				"queries must include at least a column family and a search key, got: %s",
				input)
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
				return nil, newError(errInvalidFormat, "latest must be a number. received %s",
					value)
			}
			if n < 0 {
				return nil, newError(errInvalidFormat,
					"latest must be greater than 0. received %d", n)
			}
			parsed.latest = n
		case "timestamp":
			t, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, newError(errInvalidFormat, "invalid timestamp format: %s", value)
			}
			parsed.timestamp = t
		default:
			return nil, newError(errUnknownParameter, "%s", key)
		}
	}

	// Validate that at least one search key is provided
	if parsed.rowKey == "" && parsed.rowKeyPrefix == "" && parsed.rowKeyRegex == "" {
		return nil, newError(errMissingKey, "missing search key: provide one of key, prefix, "+
			"or regex")
	}

	// Validate that exactly one search key type is provided
	keyCount := 0
	if parsed.rowKey != "" {
		keyCount++
	}
	if parsed.rowKeyPrefix != "" {
		keyCount++
	}
	if parsed.rowKeyRegex != "" {
		keyCount++
	}

	if keyCount > 1 {
		return nil, newError(errInvalidFormat,
			"only one search key type allowed: provide exactly one of rowKey, rowKeyPrefix, "+
				"or rowKeyRegex")
	}

	// Family is always required
	if parsed.family == "" {
		return nil, newError(errInvalidFormat, "missing family")
	}

	return parsed, nil
}

// readRowKey reads a single row by its key and returns the requested data. If the latest filter
// is provided in the query, it will return only the latest N versions of the qualifiers.
func (r *readQuery) readRowKey(data *litetable.Data) (*litetable.Row, error) {
	// Check if the row exists
	row, exists := (*data)[r.rowKey]
	if !exists {
		return nil, fmt.Errorf("row not found: %s", r.rowKey)
	}

	// Check if the family exists
	family, exists := row[r.family]
	if !exists {
		return nil, fmt.Errorf("family not found: %s", r.family)
	}

	// Create result container
	result := &litetable.Row{
		Key:     r.rowKey,
		Columns: make(map[string]litetable.VersionedQualifier),
	}
	result.Columns[r.family] = make(litetable.VersionedQualifier)

	// If no qualifiers specified, return all qualifiers in the family
	if len(r.qualifiers) == 0 {
		for qualifier, values := range family {
			filteredValues := r.getLatestN(values, r.latest)
			// Only add qualifier if it has values after tombstone filtering
			if len(filteredValues) > 0 {
				result.Columns[r.family][qualifier] = filteredValues
			}
		}
	} else {
		// Return only requested qualifiers
		for _, qualifier := range r.qualifiers {
			values, exists := family[qualifier]
			if !exists {
				continue // Skip non-existing qualifiers
			}
			result.Columns[r.family][qualifier] = r.getLatestN(values, r.latest)
		}
	}

	return result, nil
}

// filterRowsByPrefix reads from all rows with the specified prefix and returns the requested data.
// If the latest filter is provided in the query, it will return only the latest N versions of all
// qualifiers in the family.
func (r *readQuery) filterRowsByPrefix(data *litetable.Data) (map[string]*litetable.Row, error) {
	results := make(map[string]*litetable.Row)

	for rowKey, rowData := range *data {
		prefixMatch := strings.HasPrefix(rowKey, r.rowKeyPrefix)
		if prefixMatch {
			// Skip rows that don't have the requested family
			family, exists := rowData[r.family]
			if !exists {
				continue
			}

			// Create result container for this row
			result := &litetable.Row{
				Key:     rowKey,
				Columns: make(map[string]litetable.VersionedQualifier),
			}
			result.Columns[r.family] = make(litetable.VersionedQualifier)

			// If no qualifiers specified, return all qualifiers in the family
			if len(r.qualifiers) == 0 {
				for qualifier, values := range family {
					filteredValues := r.getLatestN(values, r.latest)
					// Only add qualifier if it has values after tombstone filtering
					if len(filteredValues) > 0 {
						result.Columns[r.family][qualifier] = filteredValues
					}
				}
			} else {
				// Return only requested qualifiers
				for _, qualifier := range r.qualifiers {
					values, exists := family[qualifier]
					if !exists {
						continue // Skip non-existing qualifiers
					}
					result.Columns[r.family][qualifier] = r.getLatestN(values, r.latest)
				}
			}

			results[rowKey] = result
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no rows found with prefix: %s", r.rowKeyPrefix)
	}

	return results, nil
}

// filterRowsByRegex reads from all rows matching the specified regex and returns the requested
// data.
// If the latest filter is provided in the query, it will return only the latest N versions of all
// qualifiers in the family.
func (r *readQuery) filterRowsByRegex(data *litetable.Data) (map[string]*litetable.Row, error) {
	regex, err := regexp.Compile(r.rowKeyRegex)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	results := make(map[string]*litetable.Row)

	for rowKey, rowData := range *data {
		if regex.MatchString(rowKey) {
			// Skip rows that don't have the requested family
			family, exists := rowData[r.family]
			if !exists {
				continue
			}

			// Create result container for this row
			result := &litetable.Row{
				Key:     rowKey,
				Columns: make(map[string]litetable.VersionedQualifier),
			}
			result.Columns[r.family] = make(litetable.VersionedQualifier)

			// If no qualifiers specified, return all qualifiers in the family
			if len(r.qualifiers) == 0 {
				for qualifier, values := range family {
					filteredValues := r.getLatestN(values, r.latest)
					// Only add qualifier if it has values after tombstone filtering
					if len(filteredValues) > 0 {
						result.Columns[r.family][qualifier] = filteredValues
					}
				}
			} else {
				// Return only requested qualifiers
				for _, qualifier := range r.qualifiers {
					values, exists := family[qualifier]
					if !exists {
						continue // Skip non-existing qualifiers
					}
					result.Columns[r.family][qualifier] = r.getLatestN(values, r.latest)
				}
			}

			results[rowKey] = result
		}
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no rows found matching regex: %s", r.rowKeyRegex)
	}

	return results, nil
}

// getLatestN returns the latest N values from a slice of TimestampedValue.
func (r *readQuery) getLatestN(values []litetable.TimestampedValue, n int) []litetable.TimestampedValue {
	if len(values) == 0 {
		return nil
	}

	// Sort by timestamp descending (newest first)
	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp > values[j].Timestamp
	})

	// Filter out values based on tombstones
	var tombstoneTimestamp int64
	var hasTombstone bool
	valuesCopy := make([]litetable.TimestampedValue, 0, len(values))

	// First pass: Find the newest tombstone (if any)
	for _, v := range values {
		if v.IsTombstone {
			if !hasTombstone || v.Timestamp > tombstoneTimestamp {
				tombstoneTimestamp = v.Timestamp
				hasTombstone = true
			}
		}
	}

	// Second pass: Keep only values newer than the tombstone
	for _, v := range values {
		if !v.IsTombstone && (!hasTombstone || v.Timestamp > tombstoneTimestamp) {
			valuesCopy = append(valuesCopy, v)
		}
	}

	// If no valid values after filtering, return nil
	if len(valuesCopy) == 0 {
		return nil
	}

	// If n is 0 or greater than the length, return all values
	if n <= 0 || n >= len(valuesCopy) {
		return valuesCopy
	}

	// Otherwise, return the top n values
	return valuesCopy[:n]
}

// processFilteredData takes raw data returned from sharded storage and applies
// additional filtering based on the query parameters (family, qualifiers, latest)
func (r *readQuery) processFilteredData(data litetable.Data) map[string]*litetable.Row {
	results := make(map[string]*litetable.Row)

	for rowKey, rowData := range data {
		// Skip rows that don't have the requested family
		family, exists := rowData[r.family]
		if !exists {
			continue
		}

		// Create result container for this row
		result := &litetable.Row{
			Key:     rowKey,
			Columns: make(map[string]litetable.VersionedQualifier),
		}
		result.Columns[r.family] = make(litetable.VersionedQualifier)

		// If no qualifiers specified, return all qualifiers in the family
		if len(r.qualifiers) == 0 {
			for qualifier, values := range family {
				filteredValues := r.getLatestN(values, r.latest)
				// Only add qualifier if it has values after tombstone filtering
				if len(filteredValues) > 0 {
					result.Columns[r.family][qualifier] = filteredValues
				}
			}
		} else {
			// Return only requested qualifiers
			for _, qualifier := range r.qualifiers {
				values, exists := family[qualifier]
				if !exists {
					continue // Skip non-existing qualifiers
				}
				filteredValues := r.getLatestN(values, r.latest)
				if len(filteredValues) > 0 {
					result.Columns[r.family][qualifier] = filteredValues
				}
			}
		}

		// Only add non-empty rows to results
		if len(result.Columns[r.family]) > 0 {
			results[rowKey] = result
		}
	}

	return results
}
