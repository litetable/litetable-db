package protocol

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"sort"
	"strconv"
	"strings"
	"time"
)

type DeleteParams struct {
	Query              []byte
	Data               *DataFormat
	ConfiguredFamilies []string
}

// Delete marks data for deletion in the store using tombstones
func (m *Manager) Delete(params *DeleteParams) error {
	// Parse the query
	parsed, err := parseDeleteQuery(string(params.Query))
	if err != nil {
		return err
	}

	// Check if the row exists
	row, exists := (*params.Data)[parsed.rowKey]
	if !exists {
		return fmt.Errorf("row not found: %s", parsed.rowKey)
	}

	// BigTable-like approach: Add tombstone markers
	modifiedFamilies := make(map[string]bool)

	if parsed.family == "" {
		// Mark the entire row for deletion by adding tombstones to all families
		for familyName, family := range row {
			for qualifier := range family {
				m.addTombstone(row, familyName, qualifier, parsed.timestamp, parsed.ttl)
			}
			modifiedFamilies[familyName] = true
		}
	} else {
		family, exists := row[parsed.family]
		if !exists {
			return fmt.Errorf("family not found: %s", parsed.family)
		}

		if len(parsed.qualifiers) == 0 {
			// Mark entire family for deletion
			for qualifier := range family {
				m.addTombstone(row, parsed.family, qualifier, parsed.timestamp, parsed.ttl)
			}
		} else {
			// Mark specific qualifiers
			for _, qualifier := range parsed.qualifiers {
				if _, exists := family[qualifier]; exists {
					m.addTombstone(row, parsed.family, qualifier, parsed.timestamp, parsed.ttl)
				}
			}
		}
		modifiedFamilies[parsed.family] = true
	}

	// Check if the row is fully tombstoned
	if isRowFullyTombstoned(row) {
		fmt.Printf("Row %s fully tombstoned; removing from memory", parsed.rowKey)
		delete(*params.Data, parsed.rowKey)
	}

	return nil
}

// addTombstone adds a tombstone marker for a cell at the passed in timestamp.
// expiresAt is a time that is configured within the Litetable configuration, but
// can be overridden with a provided TTL.
func (m *Manager) addTombstone(
	row map[string]litetable.VersionedQualifier,
	family,
	qualifier string,
	timestamp time.Time,
	ttl time.Duration,
) {

	expiresAt := time.Time{}
	// if a ttl is applied, add it to the expiration time
	if ttl > 0 {
		expiresAt = timestamp.Add(ttl)
	}

	values := row[family][qualifier]

	// Insert the tombstone
	values = append(values, litetable.TimestampedValue{
		Value:       nil,
		Timestamp:   timestamp,
		IsTombstone: true,
		ExpiresAt:   expiresAt,
	})

	// Sort versions descending by Timestamp
	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp.After(values[j].Timestamp)
	})

	// we are iterating on the actual memory map here.
	row[family][qualifier] = values
}

type deleteQuery struct {
	rowKey     string
	family     string
	qualifiers []string
	timestamp  time.Time // this is either the current time or the provided timestamp
	ttl        time.Duration
}

func parseDeleteQuery(input string) (*deleteQuery, error) {
	parts := strings.Fields(input)
	parsed := &deleteQuery{
		qualifiers: []string{},
		ttl:        0, // Default to no automatic expiration
	}

	ttlProvided := false
	timestampProvided := false

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
			timestampProvided = true
			t, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp format: %s", value)
			}
			parsed.timestamp = t.Add(time.Nanosecond) // make a slight increment in the time
		case "ttl":
			ttlProvided = true
			ttlSec, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid ttl value: %s", value)
			}
			parsed.ttl = time.Duration(ttlSec) * time.Second
		default:
			return nil, fmt.Errorf("unknown parameter: %s", key)
		}
	}

	// if a timestamp is not provided, all records before this record are deleted
	if !timestampProvided {
		parsed.timestamp = time.Now()
	}

	// enforce ttl is required
	if !ttlProvided {
		// use a default TTL
		parsed.ttl = time.Duration(3600) * time.Second // 1-hour default
	}

	// Validate required fields
	if parsed.rowKey == "" {
		return nil, fmt.Errorf("missing key")
	}

	return parsed, nil
}

func isRowFullyTombstoned(row map[string]litetable.VersionedQualifier) bool {
	for _, family := range row {
		for _, versions := range family {
			if len(versions) == 0 {
				continue
			}

			// Sort in descending order by timestamp (just to be safe)
			sort.SliceStable(versions, func(i, j int) bool {
				return versions[i].Timestamp.After(versions[j].Timestamp)
			})

			// Check if the latest version is a tombstone
			if !versions[0].IsTombstone {
				return false
			}
		}
	}
	return true
}
