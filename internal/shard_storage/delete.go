package shard_storage

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/cdc_emitter"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/shard_storage/reaper"
	"github.com/rs/zerolog/log"
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
					key,
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
					key,
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
					key,
					family,
					q,
					timestamp,
					expiresAt,
				)
			}
		}
	}

	// Mark the row as changed
	m.MarkRowChanged(family, key)

	// Send the delete data to the shard reaper
	m.reaper.Reap(&reaper.ReapParams{
		RowKey:     key,
		Family:     family,
		Qualifiers: qualifiers,
		Timestamp:  timestamp,
		ExpiresAt:  *expiresAt,
	})
	return nil
}

// addTombstone adds a tombstone marker for a cell at the passed in timestamp.
// expiresAt is a time that is configured within the Litetable configuration, but
// can be overridden with a provided TTL.
func (m *Manager) addTombstone(
	row map[string]litetable.VersionedQualifier,
	key,
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
		RowKey:    key,
		Family:    family,
		Qualifier: qualifier,
		Column:    tombstone,
	})
}

// DeleteExpiredTombstones removes expired tombstones and returns true if changes were made
func (m *Manager) DeleteExpiredTombstones(rowKey, family string, qualifiers []string, timestamp time.Time) bool {
	// Determine which shard this row belongs to
	shardIdx := m.getShardIndex(rowKey)
	sh := m.shardMap[shardIdx]

	sh.mutex.Lock()
	defer sh.mutex.Unlock()

	// Check if the row exists
	row, exists := sh.data[rowKey]
	if !exists {
		log.Debug().Msgf("Row %s does not exist", rowKey)
		return true
	}

	// Check if the family exists
	familyData, exists := row[family]
	if !exists {
		log.Debug().Msgf("Family %s does not exist in row %s", family, rowKey)
		return true
	}

	changed := false

	// if we have no qualifiers, we should GC the entire family
	if len(qualifiers) == 0 {
		delete(row, family)
		changed = true
	} else {
		// For any qualifier in the params, we should parse and compare timestamps
		for _, qualifier := range qualifiers {
			values, ex := familyData[qualifier]
			if !ex {
				log.Debug().Msgf("Qualifier %s does not exist in family %s", qualifier, family)
				continue
			}

			// Filter out entries with timestamp ≤ params.Timestamp
			var remainingValues []litetable.TimestampedValue
			for _, entry := range values {
				// save the relevant entries
				if entry.Timestamp.After(timestamp) {
					remainingValues = append(remainingValues, entry)
				} else {
					changed = true
				}
			}

			// Update the qualifier with filtered values or remove it if empty
			if len(remainingValues) > 0 {
				familyData[qualifier] = remainingValues
			} else {
				delete(familyData, qualifier)
				changed = true
			}
		}

		// Clean up empty structures
		if len(familyData) == 0 {
			delete(row, family)
		}
	}

	// If there is no data in the row key, it does not need to exist
	if len(row) == 0 {
		delete(sh.data, rowKey)
	}

	return changed
}
