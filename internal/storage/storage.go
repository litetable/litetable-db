package storage

import (
	"github.com/litetable/litetable-db/internal/litetable"
	"path/filepath"
)

// backgroundFlush periodically flushes data to disk
func (m *Manager) backgroundFlush() {
	m.mutex.Lock()
	_ = m.saveSnapshot()
	m.maintainSnapshotLimit()
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

	if err := m.loadFromLatestSnapshot(); err != nil {
		return err
	}

	return nil
}

func (m *Manager) Stop() error {
	// stop should flush data to disk
	if m.snapshotTimer != nil {
		m.snapshotTimer.Stop()
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

func (m *Manager) RWLock() {
	m.mutex.Lock()
}

func (m *Manager) RWUnlock() {
	m.mutex.Unlock()
}
