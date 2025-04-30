package storage

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func (m *Manager) saveSnapshot() error {
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

func (m *Manager) loadFromLatestSnapshot() error {
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

// maintainSnapshotLimit checks the number of snapshot files in the directory and prunes the oldest
// ones if the limit is exceeded.
func (m *Manager) maintainSnapshotLimit() {
	// List all snapshot files
	files, err := filepath.Glob(filepath.Join(m.dataDir, "snapshot-*.db"))
	if err != nil {
		fmt.Printf("Failed to list snapshot files: %v\n", err)
		return
	}

	// If we're under the limit, no pruning needed
	if len(files) <= m.maxSnapshotLimit {
		return
	}

	// Sort files by name (which contains timestamp)
	// This works because the timestamp format ensures lexicographical sorting matches chronological order
	sort.Strings(files)

	// Delete the oldest files, keeping only the configured limit
	for i := 0; i < len(files)-m.maxSnapshotLimit; i++ {
		if err := os.Remove(files[i]); err != nil {
			fmt.Printf("Failed to remove old snapshot %s: %v\n", files[i], err)
		} else {
			fmt.Printf("Pruned old snapshot: %s\n", files[i])
		}
	}
}
