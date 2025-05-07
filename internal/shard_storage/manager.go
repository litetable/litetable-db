package shard_storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

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
	data    litetable.Data
	mutex   sync.RWMutex

	backupTimer      time.Duration
	maxSnapshotLimit int

	allowedFamilies []string // Maps family names to allowed columns
	familiesFile    string   // Path to store allowed family configuration

	// create a house for the snapshot process
	changedRows               map[string]map[string]struct{} // initialized when first row is marked
	snapshotTimer             time.Duration
	lastSnapshotTime          time.Time
	lastPartialSnapshotTime   time.Time
	latestPartialSnapshotFile string
	snapshotDir               string

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
	return errors.Join(errGrp...)
}

// New creates a new disk storage manager
func New(cfg *Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	backupDir := filepath.Join(cfg.RootDir, backupDirName)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	snapDir := filepath.Join(cfg.RootDir, snapshotDir)
	if err := os.MkdirAll(snapDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if cfg.ShardCount == 0 {
		cfg.ShardCount = defaultShardCount
	}

	m := &Manager{
		rootDir:          cfg.RootDir,
		dataDir:          backupDir,
		data:             make(litetable.Data),
		snapshotTimer:    time.Duration(cfg.SnapshotTimer) * time.Second,
		backupTimer:      time.Duration(cfg.FlushThreshold) * time.Second,
		allowedFamilies:  make([]string, 0),
		familiesFile:     filepath.Join(cfg.RootDir, dataFamilyLockFile),
		maxSnapshotLimit: cfg.MaxSnapshotLimit,
		snapshotDir:      snapDir,
		mutex:            sync.RWMutex{},
		procCtx:          ctx,
		ctxCancel:        cancel,

		shardCount: defaultShardCount,
	}

	// load any existing column families
	if err := m.loadAllowedFamilies(); err != nil {
		return nil, fmt.Errorf("failed to load allowed families: %w", err)
	}

	// create the shards
	shards, err := initializeDataShards(&shardConfig{
		count:           cfg.ShardCount,
		allowedFamilies: m.allowedFamilies,
	})
	if err != nil {
		return nil, err
	}

	// assign the shards to the manager
	m.shardMap = make([]*shard, cfg.ShardCount)
	for i := 0; i < cfg.ShardCount; i++ {
		m.shardMap[i] = shards[i]
	}

	return m, nil
}

// Start initializes disk storage for the manager.
func (m *Manager) Start() error {

	// TODO: load from backup must load data into the shards
	if err := m.loadFromLatestBackup(); err != nil {
		return err
	}

	// TODO: start each shard in a separate go routine
	// each shard should have their own shutdown process

	return nil
}

// Stop is a blocking operation that flushes any remaining data to a snapshot before
// allowing the process to shut down.
func (m *Manager) Stop() error {
	if m.ctxCancel != nil {
		m.ctxCancel()
	}

	return nil
}

func (m *Manager) Name() string {
	return "Shard Storage"
}

// GetData Provides access to the data
func (m *Manager) GetData() *litetable.Data {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return &m.data
}

func (m *Manager) FamilyLockFile() string {
	return filepath.Join(m.rootDir, dataFamilyLockFile)
}

func (m *Manager) RWLock() {
	m.mutex.Lock()
}

func (m *Manager) RWUnlock() {
	m.mutex.Unlock()
}

func (m *Manager) loadAllowedFamilies() error {
	data, err := os.ReadFile(m.familiesFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, not an error
			return nil
		}
		return fmt.Errorf("failed to read allowed families file: %w", err)
	}

	return json.Unmarshal(data, &m.allowedFamilies)
}

// loadFromLatestBackup loads the latest backup file into the data cache.
func (m *Manager) loadFromLatestBackup() error {
	start := time.Now()
	latest, err := m.getLatestBackup()
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	if latest == "" {
		// No snapshot files found; initialize with empty data
		m.data = make(litetable.Data)
		return nil
	}

	dataBytes, err := os.ReadFile(latest)
	if err != nil {
		return fmt.Errorf("failed to read snapshot %s: %w", latest, err)
	}

	var loadedData litetable.Data
	if err := json.Unmarshal(dataBytes, &loadedData); err != nil {
		return fmt.Errorf("failed to parse snapshot %s: %w", latest, err)
	}

	m.data = loadedData

	log.Debug().Str("duration", time.Since(start).String()).Msg("Data loaded from backup")
	return nil
}

// getLatestBackup returns the latest full-snapshot file in the data directory.
func (m *Manager) getLatestBackup() (string, error) {
	files, err := filepath.Glob(filepath.Join(m.dataDir, backupFileGlob))
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		// No snapshots yet, nothing to load
		return "", nil
	}

	// Find the newest snapshot file
	latest := files[0]
	for _, file := range files {
		if file > latest {
			latest = file
		}
	}

	return latest, nil
}
