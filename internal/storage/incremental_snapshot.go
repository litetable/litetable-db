package storage

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"os"
	"path/filepath"
	"time"
)

type snapShopData struct {
	Version           int                                      `json:"version"`
	SnapshotTimestamp time.Time                                `json:"snapshotTimestamp"`
	IsPartial         bool                                     `json:"isPartial"`
	SnapshotData      map[string]*litetable.VersionedQualifier `json:"snapshotData"`
}

// runIncrementalSnapshot needs to take every rowKey and family from the changeRows map
// and get the contents of the row in memory and save it to the snapshot file.
func (m *Manager) runIncrementalSnapshot() error {
	// skip if nothing to do
	if len(m.changedRows) == 0 {
		return nil
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	// lock the mutex to prevent any changes to the data while we are creating the snapshot
	snapshot := m.createSnapshotData()
	if snapshot == nil {
		fmt.Println("no snapshot data to save")
		return nil
	}

	// create a partial snapshot
	prefix := "snapshot-incr"
	filename := filepath.Join(m.snapshotDir, fmt.Sprintf("%s-%d.db", prefix, time.Now().UnixNano()))

	// Serialize and save to disk
	dataBytes, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot: %w", err)
	}

	if err = os.WriteFile(filename, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	// back up the snapshot information and reset the changed rows
	m.lastSnapshotTime = time.Now()
	m.latestSnapshotFile = filename
	m.changedRows = make(map[string]map[string]struct{})

	return nil
}

// createSnapshotData creates a snapshot of the current data in memory
func (m *Manager) createSnapshotData() *snapShopData {

	snapshot := &snapShopData{
		Version:           1,
		SnapshotTimestamp: time.Now(),
		IsPartial:         true,
		SnapshotData:      make(map[string]*litetable.VersionedQualifier),
	}

	data := m.data
	for k, columnFamilies := range m.changedRows {
		// check to see if the key exists in the data, if it doesn't just continue on
		row, ok := data[k]
		if !ok {
			fmt.Printf("row %s does not exist in data\n", k)
			continue
		}

		vq := make(litetable.VersionedQualifier)
		snapshot.SnapshotData[k] = &vq

		// check to see if the family exists in on the row, if it doesn't just continue on
		for fam := range columnFamilies {
			family, exists := row[fam]
			if !exists {
				fmt.Printf("family %s does not exist in row %s\n", fam, k)
				continue
			}

			// Copy each qualifier from this family to the versioned qualifier
			for qualifier, values := range family {
				vq[qualifier] = values
			}
		}
	}

	return snapshot
}
