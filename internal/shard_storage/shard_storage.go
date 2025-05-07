// Package shard_storage is attempting to fix the lock contention issue noticed in light load
// testing.
//
// Sharding will require us to implement a predictable sharding strategy on data.
// The key requirement here is that shard scaling should be configurable on init.
//
// - each shard has their own batch of data in memory
// - each shard has their own Reaper (garbage collector)
// - each shard manages their own snapshot (all snapshots are still merged in the same process)
// - each shard has their own Lock
//
// When Start() is called, we will run each shard in a separate goroutine.
// When a consumer calls Data(),
// we will compute the shard based on the key and query the appropriate memory shard.
//
// Prefix and Regex queries are another problem entirely and are expensive.
// Because rowKeys have no known prefix, we have to scan over all shards, which locks all shards
// for a time and is annoying. This is the compromise between extremely fast read/write times
// and flexible query filters.
package shard_storage

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// shard is a manager for a single shard of in-memory litetable.Data.
type shard struct {
	data  litetable.Data
	mutex sync.RWMutex

	allowedFamilies []string // Maps family names to allowed columns

	// there should always be some degree of randomness to the backup timer to prevent all shards
	// backing up in the same timeframe.
	backupTimer      time.Duration
	maxSnapshotLimit int

	// each shard must monitor their own changes for the snapshot
	changedRows map[string]map[string]struct{} // initialized when first row is marked

	// Track if this shard has been initialized with data
	initialized atomic.Bool
}

type shardConfig struct {
	count           int
	allowedFamilies []string
}

// initializeDataShards creates and initializes new shards based on the provided configuration.
func initializeDataShards(cfg *shardConfig) ([]*shard, error) {
	if cfg.count <= 0 {
		return nil, fmt.Errorf("shard count must be greater than 0")
	}

	shards := make([]*shard, cfg.count)

	for i := 0; i < cfg.count; i++ {
		// Create a new shard with default values
		shards[i] = &shard{
			data:            make(litetable.Data),
			mutex:           sync.RWMutex{},
			allowedFamilies: cfg.allowedFamilies,
			changedRows:     make(map[string]map[string]struct{}),

			// Add small random jitter to backup timers to prevent all shards
			// from backing up simultaneously (between 0-500ms)
			backupTimer: time.Duration(i*100+rand.Intn(500)) * time.Millisecond,
		}
	}

	return shards, nil
}

// getShardIndex determines which shard a particular row key belongs to.
// It uses a consistent hashing approach to distribute keys evenly across shards.
func (m *Manager) getShardIndex(rowKey string) int {
	if m.shardCount <= 0 {
		return 0
	}

	// Use FNV-1a hash algorithm for distributing keys
	h := fnv.New32a()
	_, _ = h.Write([]byte(rowKey))
	hash := h.Sum32()

	// Modulo to get shard index within range
	return int(hash % uint32(m.shardCount))
}

func (s *shard) setInitialized() {
	s.initialized.Store(true)
}
