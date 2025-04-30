package storage

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"os"
	"path/filepath"
	"time"
)

// backgroundFlush periodically flushes data to disk
func (m *Manager) backgroundFlush() {
	m.mutex.Lock()
	_ = m.SaveSnapshot()
	m.mutex.Unlock()

	// Reset timer
	m.snapshotTimer.Reset(m.snapshotDuration)
}

func (m *Manager) FamilyLockFile() string {
	return filepath.Join(m.rootDir, dataFamilyLockFile)
}

// Start initializes disk storage for the manager.
func (m *Manager) Start() error {
	// start should load data into memory
	m.mutex.Lock()
	defer m.mutex.Unlock()

	files, err := filepath.Glob(filepath.Join(m.dataDir, "snapshot-*.db"))
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

	m.latestSnapshotFile = latest
	dataBytes, err := os.ReadFile(latest)
	if err != nil {
		return fmt.Errorf("failed to read snapshot %s: %w", latest, err)
	}

	var loadedData litetable.Data
	if err := json.Unmarshal(dataBytes, &loadedData); err != nil {
		return fmt.Errorf("failed to parse snapshot %s: %w", latest, err)
	}

	m.data = loadedData

	return nil
}

func (m *Manager) Stop() error {
	// stop should flush data to disk
	if m.snapshotTimer != nil {
		m.snapshotTimer.Stop()
	}

	// Flush any remaining data
	return m.SaveSnapshot()
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

func (m *Manager) SaveSnapshot() error {
	filename := filepath.Join(m.dataDir, fmt.Sprintf("snapshot-%d.db", time.Now().UnixNano()))

	m.mutex.Lock()
	defer m.mutex.Unlock()

	dataBytes, err := json.Marshal(m.data)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot: %w", err)
	}

	if err = os.WriteFile(filename, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	return nil
}

func (m *Manager) RWLock() {
	m.mutex.Lock()
}

func (m *Manager) RWUnlock() {
	m.mutex.Unlock()
}
