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
	dataDiskName       = ".table"
	snapshotDir        = "snapshots"
	dataFamilyLockFile = "families.config.json"
)

var (
	standardSnapshotPruneTime = 1 // TODO: make this not run every minute
	defaultSnapshotLimit      = 10
)

// Manager handles persistent storage operations to a disk
type Manager struct {
	rootDir string
	dataDir string
	data    litetable.Data
	mutex   sync.RWMutex

	snapshotDuration time.Duration
	maxSnapshotLimit int

	allowedFamilies []string // Maps family names to allowed columns
	familiesFile    string   // Path to store allowed family configuration

	latestSnapshotFile string

	// create a house for the snapshot process
	changedRows               map[string]map[string]struct{} // initialized when first row is marked
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

	dirName := filepath.Join(cfg.RootDir, dataDiskName)
	if err := os.MkdirAll(dirName, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	snapDir := filepath.Join(dirName, snapshotDir)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}
	// if we got nothing, set the default
	if cfg.MaxSnapshotLimit == 0 {
		cfg.MaxSnapshotLimit = defaultSnapshotLimit
	}

	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		rootDir:          cfg.RootDir,
		dataDir:          dirName,
		data:             make(litetable.Data),
		snapshotDuration: time.Duration(cfg.FlushThreshold) * time.Minute,
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

	if err := m.loadFromLatestSnapshot(); err != nil {
		return err
	}

	// Start the background process for snapshots
	go func() {
		ticker := time.NewTicker(m.snapshotDuration)
		pruneTicker := time.NewTicker(time.Duration(standardSnapshotPruneTime) * time.Minute)

		defer func() {
			ticker.Stop()
			pruneTicker.Stop()
		}()

		for {
			select {
			case <-m.procCtx.Done():
				return
			case <-ticker.C:
				err := m.saveSnapshot()
				if err != nil {
					fmt.Printf("failed to save snapshot: %v\n", err)
				}
			case <-pruneTicker.C:
				m.maintainSnapshotLimit()
			}
		}
	}()
	return nil
}

func (m *Manager) Stop() error {
	if m.ctxCancel != nil {
		m.ctxCancel()
	}

	// Flush any remaining data
	return m.saveSnapshot()
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
