package shard_storage

import (
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/rs/zerolog/log"
	"regexp"
	"strings"
	"sync"
)

// GetRowByFamily returns the data attached to a row key and family: this would be a
// litetable.VersionedQualifier.
func (m *Manager) GetRowByFamily(key, family string) (*litetable.VersionedQualifier, bool) {
	// find the shard index
	shardKey := m.getShardIndex(key)

	// get the shard
	s := m.shardMap[shardKey]

	// lock the shard
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	// get the row
	row, exists := s.data[key]
	if !exists {
		return nil, false
	}

	r := make(litetable.VersionedQualifier)

	// Check if the family exists
	r, exists = row[family]
	if !exists {
		return nil, false
	}

	log.Debug().Msgf("found row %s in shard %d", key, shardKey)
	// return the row
	return &r, true
}

// FilterRowsByPrefix has to query all shards to find all rows that match the data. Prefix queries
// are expensive in that they require locking all shards and scanning all data.
func (m *Manager) FilterRowsByPrefix(prefix string) (*litetable.Data, bool) {
	result := make(litetable.Data)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	matchFound := false

	wg.Add(len(m.shardMap))

	for _, s := range m.shardMap {
		go func(shard *shard) {
			defer wg.Done()

			// Local results for this shard
			localMatches := make(litetable.Data)
			localFound := false

			shard.RLock()
			for rowKey, rowData := range shard.data {
				if strings.HasPrefix(rowKey, prefix) {
					localMatches[rowKey] = rowData
					localFound = true
				}
			}
			shard.RUnlock()

			// If we found matches, merge them into the result under lock
			if localFound {
				mutex.Lock()
				for k, v := range localMatches {
					result[k] = v
				}
				matchFound = matchFound || localFound
				mutex.Unlock()
			}
		}(s)
	}

	wg.Wait()
	return &result, matchFound
}

func (m *Manager) FilterRowsByRegex(regex string) (*litetable.Data, bool) {
	result := make(litetable.Data)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	matchFound := false

	// Compile regex once, outside the goroutines
	reg, err := regexp.Compile(regex)
	if err != nil {
		// If regex is invalid, return empty result
		return &result, false
	}

	wg.Add(len(m.shardMap))

	for _, s := range m.shardMap {
		go func(shard *shard) {
			defer wg.Done()

			// Local results for this shard
			localMatches := make(litetable.Data)
			localFound := false

			shard.RLock()
			for rowKey, rowData := range shard.data {
				if reg.MatchString(rowKey) {
					localMatches[rowKey] = rowData
					localFound = true
				}
			}
			shard.RUnlock()

			// If we found matches, merge them into the result under lock
			if localFound {
				mutex.Lock()
				for k, v := range localMatches {
					result[k] = v
				}
				matchFound = matchFound || localFound
				mutex.Unlock()
			}
		}(s)
	}

	wg.Wait()
	return &result, matchFound
}
