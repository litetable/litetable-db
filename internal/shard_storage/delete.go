package shard_storage

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"sort"
	"time"
)

func (m *Manager) Delete(key, family string, qualifiers []string, timestamp time.Time,
	expiresAt *time.Time) error {
	// find the shard index
	shardKey := m.getShardIndex(key)

	// get the shard
	s := m.shardMap[shardKey]

	// lock the shard
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if the row exists
	row, exists := s.data[key]
	if !exists {
		return fmt.Errorf("row not found: %s", key)
	}

	// if the family is empty, we should mark the entire row key for garbage collection
	if family == "" {
		for familyName, quals := range row {
			// for every qualifier insert a tombstone at the time
			for q := range quals {
				fmt.Println("Adding tombstone to qualifier:", q, familyName)
				// add tombstone markers to all qualifiers
				m.addTombstone(
					row,
					familyName,
					q,
					timestamp,
					expiresAt,
				)
			}
		}
	} else {
		// if the family is specified, we will only append tombstones based on if qualifiers
		// are provided
		// validate the family and make sure it exists
		if !m.IsFamilyAllowed(family) {
			return fmt.Errorf("family not allowed: %s", family)
		}
		fam, exists := row[family]
		if !exists {
			return fmt.Errorf("family %s not found on key: %s", family, key)
		}

		// if there are no provided qualifers, we should mark the whole family for deletion
		if len(qualifiers) == 0 {
			// Mark entire family for deletion
			for q := range fam {
				fmt.Println("Adding tombstone to qualifier:", q, family)
				m.addTombstone(
					row,
					family,
					q,
					timestamp,
					expiresAt,
				)

				// TODO: all tombstones should send a CDC event
			}
		} else {
			for _, q := range qualifiers {
				fmt.Println("Adding tombstone to qualifier:", q, family)
				m.addTombstone(
					row,
					family,
					q,
					timestamp,
					expiresAt,
				)
			}
		}
	}
	
	// TODO - emit any changes and trigger garbage collection
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
	expiresAt *time.Time,
) {
	values := row[family][qualifier]

	tombstone := litetable.TimestampedValue{
		Value:       nil,
		Timestamp:   timestamp,
		IsTombstone: true,
		ExpiresAt:   *expiresAt,
	}

	// Insert the tombstone
	values = append(values, tombstone)

	// Sort versions descending by Timestamp
	sort.Slice(values, func(i, j int) bool {
		return values[i].Timestamp.After(values[j].Timestamp)
	})

	// we are iterating on the actual memory map here.
	row[family][qualifier] = values

	// m.cdc.Emit(&cdc_emitter.CDCParams{
	// 	Operation: litetable.OperationDelete,
	// 	RowKey:    rowKey,
	// 	Column:    tombstone,
	// })
}
