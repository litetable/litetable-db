package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	backupDirName      = ".table_backup"
	snapshotDir        = ".snapshots"
	dataFamilyLockFile = "families.config.json"
)

var (
	standardSnapshotPruneTime = 1 // TODO: make this not run every minute
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
}

type Config struct {
	RootDir          string
	FlushThreshold   int
	SnapshotTimer    int
	MaxSnapshotLimit int
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
	}

	// load any existing column families
	if err := m.loadAllowedFamilies(); err != nil {
		return nil, fmt.Errorf("failed to load allowed families: %w", err)
	}

	return m, nil
}

// Start initializes disk storage for the manager.
func (m *Manager) Start() error {

	// load the latest backup to initialize the cached data
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
				err := m.runIncrementalSnapshot()
				if err != nil {
					fmt.Printf("failed to save snapshot: %v\n", err)
				}
			case <-snapshotMerge.C:
				err := m.snapshotMerge()
				if err != nil {
					fmt.Printf("failed to merge snapshot: %v\n", err)
				}
			case <-pruneTicker.C:
				m.maintainSnapshotLimit()
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
	err := m.runIncrementalSnapshot()
	if err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}

	// create a backup - this could take time
	return m.snapshotMerge()
}

func (m *Manager) Name() string {
	return "Disk Storage"
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
