package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/protocol"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	dataDiskName       = ".table"
	dataFamilyLockFile = "config.families.json"
)

// Disk handles persistent storage operations to a disk
type Disk struct {
	rootDir string
	dataDir string
	data    protocol.DataFormat
	lock    sync.RWMutex

	snapshotDuration time.Duration
	snapshotTimer    *time.Timer
}

type Config struct {
	RootDir        string
	FlushThreshold int
}

func (c *Config) validate() error {
	var errGrp []error
	if c.RootDir == "" {
		errGrp = append(errGrp, fmt.Errorf("data directory is required"))
	}
	if c.FlushThreshold <= 0 {
		errGrp = append(errGrp, fmt.Errorf("flush threshold must be greater than 0"))
	}

	return errors.Join(errGrp...)
}

// NewDiskStorage creates a new disk storage manager
func NewDiskStorage(cfg *Config) (*Disk, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	dirName := filepath.Join(cfg.RootDir, dataDiskName)
	if err := os.MkdirAll(dirName, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	ds := &Disk{
		rootDir:          cfg.RootDir,
		dataDir:          dirName,
		data:             make(protocol.DataFormat),
		snapshotDuration: time.Duration(cfg.FlushThreshold) * time.Second,
	}

	// Start background flush timer
	ds.snapshotTimer = time.AfterFunc(ds.snapshotDuration, ds.backgroundFlush)

	return ds, nil
}

// backgroundFlush periodically flushes data to disk
func (ds *Disk) backgroundFlush() {
	ds.lock.Lock()
	_ = ds.SaveSnapshot()
	ds.lock.Unlock()

	// Reset timer
	ds.snapshotTimer.Reset(ds.snapshotDuration)
}

func (ds *Disk) FamilyLockFile() string {
	return filepath.Join(ds.rootDir, dataFamilyLockFile)
}

// Start initializes disk storage for the manager.
func (ds *Disk) Start() error {
	// start should load data into memory
	ds.lock.Lock()
	defer ds.lock.Unlock()

	files, err := filepath.Glob(filepath.Join(ds.dataDir, "snapshot-*.db"))
	if err != nil {
		return fmt.Errorf("failed to list snapshot files: %w", err)
	}

	if len(files) == 0 {
		// No snapshots yet, nothing to load
		return nil
	}

	// Find the newest snapshot file
	latest := files[0]
	for _, file := range files {
		if file > latest {
			latest = file
		}
	}

	dataBytes, err := os.ReadFile(latest)
	if err != nil {
		return fmt.Errorf("failed to read snapshot %s: %w", latest, err)
	}

	var loadedData protocol.DataFormat
	if err := json.Unmarshal(dataBytes, &loadedData); err != nil {
		return fmt.Errorf("failed to parse snapshot %s: %w", latest, err)
	}

	ds.data = loadedData

	return nil
}

func (ds *Disk) Stop() error {
	// stop should flush data to disk
	if ds.snapshotTimer != nil {
		ds.snapshotTimer.Stop()
	}

	// Flush any remaining data
	return ds.SaveSnapshot()
}

func (ds *Disk) Name() string {
	return "disk-storage"
}

// GetData Provides access to the data
func (ds *Disk) GetData() *protocol.DataFormat {
	ds.lock.RLock()
	defer ds.lock.RUnlock()
	return &ds.data
}

func (ds *Disk) SaveSnapshot() error {
	ds.lock.RLock()
	defer ds.lock.RUnlock()

	filename := filepath.Join(ds.dataDir, fmt.Sprintf("snapshot-%d.db", time.Now().UnixNano()))

	dataBytes, err := json.Marshal(ds.data)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot: %w", err)
	}

	if err := os.WriteFile(filename, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	return nil
}
