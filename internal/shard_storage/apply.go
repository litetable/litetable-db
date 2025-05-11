package shard_storage

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/cdc_emitter"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/shard_storage/reaper"
	"github.com/rs/zerolog/log"
)

func (m *Manager) Apply(rowKey, family string, qualifiers []string, values [][]byte,
	timestamp int64, expiresAt int64) error {
	// Check if the family is allowed
	if !m.IsFamilyAllowed(family) {
		return fmt.Errorf("column family not allowed: %s", family)
	}

	// find the shard index
	shardKey := m.getShardIndex(rowKey)

	// get the shard
	s := m.shardMap[shardKey]

	// lock the shard
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Ensure data structures exist
	if s.data == nil {
		s.data = make(map[string]map[string]litetable.VersionedQualifier)
	}

	if _, exists := s.data[rowKey]; !exists {
		s.data[rowKey] = make(map[string]litetable.VersionedQualifier)
	}

	if _, exists := s.data[rowKey][family]; !exists {
		s.data[rowKey][family] = make(map[string][]litetable.TimestampedValue)
	}

	// Write all qualifier-value pairs with the same timestamp
	for i, qualifier := range qualifiers {
		value := values[i]

		newValue := litetable.TimestampedValue{
			Value:     value,
			Timestamp: timestamp,
		}

		// If we have an expiration time, mark as tombstone
		// TODO: fix this bug. If we add a tombstone to a qualifier,
		//  it won't return from call - duh!
		if expiresAt > 0 {
			newValue.IsTombstone = true
			newValue.ExpiresAt = expiresAt
		}

		s.data[rowKey][family][qualifier] = append(
			s.data[rowKey][family][qualifier], newValue,
		)

		// Emit CDC event for each qualifier
		if m.cdc != nil {
			m.cdc.Emit(&cdc_emitter.CDCParams{
				Operation: litetable.OperationWrite,
				RowKey:    rowKey,
				Family:    family,
				Qualifier: qualifier,
				Column:    newValue,
			})
		}
	}

	// Handle garbage collection if an expiresAt time is passed
	if expiresAt > 0 {
		log.Debug().Msg("calling reaper on write operation")
		m.reaper.Reap(&reaper.ReapParams{
			RowKey:     rowKey,
			Family:     family,
			Qualifiers: qualifiers,
			Timestamp:  timestamp,
			ExpiresAt:  expiresAt,
		})
	}

	m.MarkRowChanged(family, rowKey)

	return nil
}
