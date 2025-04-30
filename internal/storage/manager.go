package storage

import (
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
	dataFamilyLockFile = "families.config.json"
)

var (
	defaultSnapshotLimit = 10
)

// Manager handles persistent storage operations to a disk
type Manager struct {
	rootDir string
	dataDir string
	data    litetable.Data
	mutex   sync.RWMutex

	snapshotDuration time.Duration
	snapshotTimer    *time.Timer
	maxSnapshotLimit int

	latestSnapshotFile string
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

	// if we got nothing, set the default
	if cfg.MaxSnapshotLimit == 0 {
		cfg.MaxSnapshotLimit = defaultSnapshotLimit
	}

	m := &Manager{
		rootDir:          cfg.RootDir,
		dataDir:          dirName,
		data:             make(litetable.Data),
		snapshotDuration: time.Duration(cfg.FlushThreshold) * time.Second,
		maxSnapshotLimit: cfg.MaxSnapshotLimit,
		mutex:            sync.RWMutex{},
	}

	// Start background flush timer
	m.snapshotTimer = time.AfterFunc(m.snapshotDuration, m.backgroundFlush)

	return m, nil
}
