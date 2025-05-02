package operations

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/cdc_emitter"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/reaper"
	"sort"
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

	data := m.storage.GetData()

	// Check if the row exists
	row, exists := (*data)[parsed.rowKey]
	if !exists {
		return fmt.Errorf("row not found: %s", parsed.rowKey)
	}

	// BigTable-like approach: Add tombstone markers
	modifiedFamilies := make(map[string]bool)

	// if a delete mutation comes without a family, the operation will apply a tombstone
	// to all qualifiers in every family.
	if parsed.family == "" {
		// Mark the entire row for deletion by adding tombstones to all qualifiers
		for familyName, family := range row {
			for qualifier := range family {
				m.addTombstone(
					row,
					parsed.rowKey,
					familyName,
					qualifier,
					parsed.timestamp,
					parsed.expiresAt)
			}
			modifiedFamilies[familyName] = true
		}
	} else {
		// if the family is specified, we will only append tombstones based on if qualifiers
		// are provided
		family, exists := row[parsed.family]
		if !exists {
			return fmt.Errorf("family not found: %s", parsed.family)
		}

		// if a family is provided without qualifiers, we mark the entire family for GC
		if len(parsed.qualifiers) == 0 {
			// Mark entire family for deletion
			for qualifier := range family {
				m.addTombstone(
					row,
					parsed.rowKey,
					parsed.family,
					qualifier,
					parsed.timestamp,
					parsed.expiresAt)
			}
		} else {
			// otherwise, we should only apply tombstones to the qualifiers that are provided
			for _, qualifier := range parsed.qualifiers {
				if _, exists := family[qualifier]; exists {
					m.addTombstone(
						row,
						parsed.rowKey,
						parsed.family,
						qualifier,
						parsed.timestamp,
						parsed.expiresAt)
				}
			}
		}
		modifiedFamilies[parsed.family] = true
	}

	// if we've made it this far, send the deleted data for garbage collection
	m.garbageCollector.Reap(&reaper.ReapParams{
		RowKey:     parsed.rowKey,
		Family:     parsed.family,
		Qualifiers: parsed.qualifiers,
		Timestamp:  parsed.timestamp,
		ExpiresAt:  parsed.expiresAt,
	})

	return nil
}

// addTombstone adds a tombstone marker for a cell at the passed in timestamp.
// expiresAt is a time that is configured within the Litetable configuration, but
// can be overridden with a provided TTL.
func (m *Manager) addTombstone(
	row map[string]litetable.VersionedQualifier,
	rowKey string,
	family,
	qualifier string,
	timestamp time.Time,
	expiresAt time.Time,
) {
	values := row[family][qualifier]

	tombstone := litetable.TimestampedValue{
		Value:       nil,
		Timestamp:   timestamp,
		IsTombstone: true,
		ExpiresAt:   expiresAt,
	}

	// Insert the tombstone
	values = append(values, tombstone)

	// Sort versions descending by Timestamp
	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp.After(values[j].Timestamp)
	})

	// we are iterating on the actual memory map here.
	row[family][qualifier] = values

	m.cdc.Emit(&cdc_emitter.CDCParams{
		Operation: litetable.OperationDelete,
		RowKey:    rowKey,
		Column:    tombstone,
	})
}

type deleteQuery struct {
	rowKey     string
	family     string
	qualifiers []string
	timestamp  time.Time // this is either the current time or the provided timestamp
	ttl        time.Duration
	expiresAt  time.Time
}

func parseDeleteQuery(input string) (*deleteQuery, error) {
	parts := strings.Fields(input)
	now := time.Now()
	parsed := &deleteQuery{
		qualifiers: []string{},
		ttl:        time.Duration(3600) * time.Second,
		timestamp:  now,
		expiresAt:  now.Add(time.Hour),
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

	// Validate required fields
	if parsed.rowKey == "" {
		return nil, fmt.Errorf("missing key")
	}

	return parsed, nil
}
