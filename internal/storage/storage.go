package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	dataDiskName = ".table"
)

// Disk handles persistent storage operations to a disk
type Disk struct {
	dataDir    string
	memTable   map[string]map[string]litetable.VersionedQualifier
	lock       sync.RWMutex
	flushSize  int
	flushTimer *time.Timer
}

type Config struct {
	DataDir        string
	FlushThreshold int
}

func (c *Config) validate() error {
	var errGrp []error
	if c.DataDir == "" {
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

	dirName := filepath.Join(cfg.DataDir, dataDiskName)
	if err := os.MkdirAll(dirName, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	ds := &Disk{
		dataDir:   dirName,
		memTable:  make(map[string]map[string]litetable.VersionedQualifier),
		flushSize: cfg.FlushThreshold,
	}

	// Start background flush timer
	ds.flushTimer = time.AfterFunc(30*time.Second, ds.backgroundFlush)

	return ds, ds.loadFromDisk()
}

// LoadFromDisk reads all data from disk files and returns the loaded data
// It's used by the Engine to populate its in-memory cache during startup
func (ds *Disk) LoadFromDisk() (map[string]map[string]litetable.VersionedQualifier, error) {
	ds.lock.RLock()
	defer ds.lock.RUnlock()

	// Return a copy of the current memTable which was loaded from disk during initialization
	result := make(map[string]map[string]litetable.VersionedQualifier)
	for rowKey, families := range ds.memTable {
		result[rowKey] = make(map[string]litetable.VersionedQualifier)
		for family, qualifier := range families {
			result[rowKey][family] = qualifier
		}
	}

	return result, nil
}

// loadFromDisk loads existing data from disk into memory
func (ds *Disk) loadFromDisk() error {
	files, err := filepath.Glob(filepath.Join(ds.dataDir, "data-*.db"))
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", file, err)
		}

		var fileData map[string]map[string]litetable.VersionedQualifier
		if err := json.Unmarshal(data, &fileData); err != nil {
			return fmt.Errorf("failed to parse file %s: %w", file, err)
		}

		// Merge data into memTable
		ds.lock.Lock()
		for rowKey, families := range fileData {
			if ds.memTable[rowKey] == nil {
				ds.memTable[rowKey] = make(map[string]litetable.VersionedQualifier)
			}
			for family, qualifier := range families {
				ds.memTable[rowKey][family] = qualifier
			}
		}
		ds.lock.Unlock()
	}

	return nil
}

// flush writes in-memory data to disk
func (ds *Disk) flush() error {
	// Create a timestamp-based filename
	filename := filepath.Join(ds.dataDir, fmt.Sprintf("data-%d.db", time.Now().UnixNano()))

	// Serialize memTable to JSON
	data, err := json.Marshal(ds.memTable)
	if err != nil {
		return fmt.Errorf("failed to serialize data: %w", err)
	}

	// Write to file
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write data file: %w", err)
	}

	// Reset memTable
	ds.memTable = make(map[string]map[string]litetable.VersionedQualifier)

	// Schedule compaction if needed
	go ds.compactIfNeeded()

	return nil
}

// backgroundFlush periodically flushes data to disk
func (ds *Disk) backgroundFlush() {
	ds.lock.Lock()
	if len(ds.memTable) > 0 {
		_ = ds.flush() // Log error if needed
	}
	ds.lock.Unlock()

	// Reset timer
	ds.flushTimer.Reset(30 * time.Second)
}

// Write adds or updates data in memory and triggers flush if needed
func (ds *Disk) Write(rowKey, family string, qualifier litetable.VersionedQualifier) error {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	// Initialize family map if needed
	if ds.memTable[rowKey] == nil {
		ds.memTable[rowKey] = make(map[string]litetable.VersionedQualifier)
	}

	// Store data
	ds.memTable[rowKey][family] = qualifier

	// Check if flush is needed
	if len(ds.memTable) >= ds.flushSize {
		return ds.flush()
	}

	return nil
}

// compactIfNeeded merges small files into larger ones
func (ds *Disk) compactIfNeeded() {
	// Implement compaction logic here
	// This would merge multiple small files into fewer larger files
	// to improve read performance
}

func (ds *Disk) ForceFlush() error {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	// used to force the cache to flush
	if len(ds.memTable) > 0 {
		return ds.flush()
	}
	return nil
}
