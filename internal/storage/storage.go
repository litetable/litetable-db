package storage

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"path/filepath"
	"time"
)

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
