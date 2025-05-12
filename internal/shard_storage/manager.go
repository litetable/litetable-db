package shard_storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/cdc_emitter"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/shard_storage/reaper"
	"github.com/rs/zerolog/log"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type cdc interface {
	Emit(params *cdc_emitter.CDCParams)
}

type garbageCollector interface {
	Reap(p *reaper.ReapParams)
}

const (
	backupDirName      = ".table_backup"
	snapshotDir        = ".snapshots"
	dataFamilyLockFile = "families.config.json"
	backupFileGlob     = "backup-*.db"
)

var (
	standardSnapshotPruneTime = 1 // TODO: make this not run every minute
	defaultShardCount         = 2
)

// Manager handles persistent storage operations to a disk
type Manager struct {
	rootDir string
	dataDir string
	mutex   sync.RWMutex

	backupTimer      time.Duration
	maxSnapshotLimit int

	allowedFamilies []string // Maps family names to allowed columns
	familiesFile    string   // Path to store allowed family configuration

	// create a house for the snapshot process
	changedRows               map[string]map[string]struct{} // initialized when first row is marked
	snapshotTimer             time.Duration
	lastPartialSnapshotTime   int64
	latestPartialSnapshotFile string
	snapshotDir               string

	// garbage collection
	reaper garbageCollector

	cdc cdc

	procCtx   context.Context
	ctxCancel context.CancelFunc

	shardCount int // The Maximum number of shards to create
	// shardMap is the locations of the running shards
	shardMap []*shard // Map of shard names to shard objects
}

type Config struct {
	RootDir          string
	FlushThreshold   int
	SnapshotTimer    int
	MaxSnapshotLimit int
	ShardCount       int
	CDCEmitter       cdc
}

func (c *Config) validate() error {
	var errGrp []error
	if c.RootDir == "" {
		errGrp = append(errGrp, fmt.Errorf("data directory is required"))
	}
	if c.FlushThreshold <= 0 {
		errGrp = append(errGrp, fmt.Errorf("flush threshold must be greater than 0"))
	}

	// if the configured snapshot is less than 1, throw an error
	if c.SnapshotTimer < 1 {
		errGrp = append(errGrp, fmt.Errorf("snapshot timer must be greater than 0"))
	}

	// if the configured snapshot is larger than 50, throw an error
	if c.MaxSnapshotLimit < 0 || c.MaxSnapshotLimit > 50 {
		errGrp = append(errGrp, fmt.Errorf("max snapshot limit must be between 1 and 50"))
	}

	if c.ShardCount < 0 || c.ShardCount > 50 {
		errGrp = append(errGrp, fmt.Errorf("shard count must be between 1 and 50"))
	}

	if c.CDCEmitter == nil {
		errGrp = append(errGrp, fmt.Errorf("CDC emitter is required"))
	}
	return errors.Join(errGrp...)
}

// New creates a new shard storage manager
func New(cfg *Config) (*Manager, *reaper.Reaper, error) {
	if err := cfg.validate(); err != nil {
		return nil, nil, err
	}

	backupDir := filepath.Join(cfg.RootDir, backupDirName)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	snapDir := filepath.Join(cfg.RootDir, snapshotDir)
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if cfg.ShardCount == 0 {
		cfg.ShardCount = defaultShardCount
	}

	log.Debug().Int("shard_count", cfg.ShardCount).Msg("Shard count")

	m := &Manager{
		rootDir:          cfg.RootDir,
		dataDir:          backupDir,
		snapshotTimer:    time.Duration(cfg.SnapshotTimer) * time.Second,
		backupTimer:      time.Duration(cfg.FlushThreshold) * time.Second,
		allowedFamilies:  make([]string, 0),
		familiesFile:     filepath.Join(cfg.RootDir, dataFamilyLockFile),
		maxSnapshotLimit: cfg.MaxSnapshotLimit,
		snapshotDir:      snapDir,
		mutex:            sync.RWMutex{},
		procCtx:          ctx,
		ctxCancel:        cancel,

		shardCount: cfg.ShardCount,
		cdc:        cfg.CDCEmitter,
	}

	// load any existing column families
	if err := m.loadAllowedFamilies(); err != nil {
		return nil, nil, fmt.Errorf("failed to load allowed families: %w", err)
	}

	// create the shards
	shards, err := initializeDataShards(&shardConfig{
		count: m.shardCount,
	})
	if err != nil {
		return nil, nil, err
	}

	// assign the shards to the manager
	m.shardMap = make([]*shard, cfg.ShardCount)
	for i := 0; i < cfg.ShardCount; i++ {
		m.shardMap[i] = shards[i]
	}

	// create a garbage collector
	gc, err := reaper.New(&reaper.Config{
		Path:       cfg.RootDir,
		Storage:    m,
		GCInterval: 10,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create garbage collector: %w", err)
	}

	m.reaper = gc
	return m, gc, nil
}

// Start initializes disk storage for the manager.
func (m *Manager) Start() error {

	// TODO: load from backup must load data into the shards
	if err := m.loadFromLatestBackup(); err != nil {
		return err
	}

	// Start the background process for snapshots
	go func() {
		snapshotTicker := time.NewTicker(m.snapshotTimer)
		// whatever the snapshot is, add 50%
		snapshotMerge := time.NewTicker(m.backupTimer + (m.backupTimer / 2))
		pruneTicker := time.NewTicker(time.Duration(standardSnapshotPruneTime) * time.Minute)

		defer func() {
			snapshotTicker.Stop()
			pruneTicker.Stop()
		}()

		for {
			select {
			case <-m.procCtx.Done():
				return
			case <-snapshotTicker.C:
				err := m.createDirectSnapshot()
				if err != nil {
					fmt.Printf("failed to save snapshot: %v\n", err)
				}
			case <-snapshotMerge.C:
				err := m.ApplyDirectSnapshots()
				if err != nil {
					fmt.Printf("failed to merge snapshot: %v\n", err)
				}
			case <-pruneTicker.C:
				m.maintainBackupLimit()
			}
		}
	}()
	return nil
}

// Stop is a blocking operation that flushes any remaining data to a snapshot before
// allowing the process to shut down.
func (m *Manager) Stop() error {
	if m.ctxCancel != nil {
		m.ctxCancel()
	}

	// Flush any remaining data
	err := m.createDirectSnapshot()
	if err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}

	// create a backup - this could take time
	return m.ApplyDirectSnapshots()
}

func (m *Manager) Name() string {
	return "Shard Storage"
}

func (m *Manager) RWLock() {
	m.mutex.Lock()
}

func (m *Manager) RWUnlock() {
	m.mutex.Unlock()
}

// distributeDataToShards takes loaded data and distributes it to the appropriate shards concurrently
// using goroutines.
//
// Depending on the size of the data, this may take some time.
// We are allocating all logical CPU cores for this task,
// which might be overkill for 90% of datasets.
func (m *Manager) distributeDataToShards(loadedData litetable.Data) error {
	// Check if any shard has already been initialized
	for i := range m.shardMap {
		if m.shardMap[i].initialized.Load() {
			return fmt.Errorf("attempted to distribute data to already initialized shards")
		}
	}

	// Use concurrency to distribute data across shards
	numWorkers := runtime.NumCPU()
	rowChan := make(chan struct {
		key      string
		families map[string]litetable.VersionedQualifier
	})
	wg := sync.WaitGroup{}

	// Initialize shards data if needed
	for i := range m.shardMap {
		m.shardMap[i].mutex.Lock()
		if m.shardMap[i].data == nil {
			m.shardMap[i].data = make(litetable.Data)
		}
		m.shardMap[i].mutex.Unlock()
	}

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range rowChan {
				// Determine which shard this row belongs to
				shardIdx := m.getShardIndex(item.key)

				// Add the data to the shard
				m.shardMap[shardIdx].mutex.Lock()
				m.shardMap[shardIdx].data[item.key] = item.families
				m.shardMap[shardIdx].mutex.Unlock()
			}
		}()
	}

	// Send data to workers
	for rowKey, families := range loadedData {
		rowChan <- struct {
			key      string
			families map[string]litetable.VersionedQualifier
		}{key: rowKey, families: families}
	}
	close(rowChan)

	// Wait for all workers to finish
	wg.Wait()

	// Mark all shards as initialized
	for i := range m.shardMap {
		m.shardMap[i].setInitialized()
	}

	return nil
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

// MarkRowChanged will save the row key and family name to the changedRows map.
//
// We are using an empty struct{} because it takes 0 bytes.
func (m *Manager) MarkRowChanged(family, rowKey string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.changedRows == nil {
		m.changedRows = make(map[string]map[string]struct{})
	}

	if _, exists := m.changedRows[rowKey]; !exists {
		m.changedRows[rowKey] = make(map[string]struct{})
	}

	// Add the family to the row key (this may be overwriting, but that's okay because
	// we just want to make sure the family is in the map)
	m.changedRows[rowKey][family] = struct{}{}
}
