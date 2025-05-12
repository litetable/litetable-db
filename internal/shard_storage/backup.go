// Package shard_storage provides the interface for data storage management.
//
// Saving to disk is fairly straight-forward, but snapshotting is a process that requires
// some consideration for how data is merged.
//
// The long-term goal with snapshotting is to support incremental snapshots with configuration for
// external replication; which would work similar to a prometheus server.
//
// Garbage collection would continue to be handled by the reaper, who would also report changes.
package shard_storage

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// saveBackup creates a new backup file with the provided data. It does not interact with the memory
// cache.
func (m *Manager) saveBackup(data *litetable.Data) error {
	start := time.Now()
	filename := filepath.Join(m.dataDir, fmt.Sprintf("backup-%d.db", start.UnixNano()))

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot: %w", err)
	}

	if err = os.WriteFile(filename, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	log.Debug().Str("duration", time.Since(start).String()).Msgf("Backup saved to %s", filename)
	return nil
}

// loadFromLatestBackup loads the latest backup file into the data cache.
func (m *Manager) loadFromLatestBackup() error {
	start := time.Now()
	latest, err := m.getLatestBackup()
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot: %w", err)
	}

	// TODO: handle case where data is empty or missing
	if latest == "" {
		log.Debug().Msg("No snapshots found, nothing to load")
		return nil
	}

	dataBytes, err := os.ReadFile(latest)
	if err != nil {
		return fmt.Errorf("failed to read snapshot %s: %w", latest, err)
	}

	var loadedData litetable.Data
	if err := json.Unmarshal(dataBytes, &loadedData); err != nil {
		return fmt.Errorf("failed to parse snapshot %s: %w", latest, err)
	}

	// Distribute data to shards concurrently, this is a blocking operation and will take some time
	// based on the size of the data set, the number of shards and the number of logical CPU cores
	// available on the system.
	if err = m.distributeDataToShards(loadedData); err != nil {
		return fmt.Errorf("failed to distribute data to shards: %w", err)
	}

	log.Debug().Str("duration", time.Since(start).String()).Msg("Data loaded from backup")
	return nil
}

// loadLatestBackup attempts to read and parse the latest backup file.
func (m *Manager) loadLatestBackup() (litetable.Data, error) {
	latest, err := m.getLatestBackup()
	if err != nil {
		return nil, fmt.Errorf("failed to get latest backup: %w", err)
	}
	if latest == "" {
		return make(litetable.Data), nil
	}

	data, err := os.ReadFile(latest)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup %s: %w", latest, err)
	}

	var parsed litetable.Data
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("failed to unmarshal backup %s: %w", latest, err)
	}
	return parsed, nil
}

// getLatestBackup returns the latest full-snapshot file in the data directory.
func (m *Manager) getLatestBackup() (string, error) {
	files, err := filepath.Glob(filepath.Join(m.dataDir, backupFileGlob))
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		// No snapshots yet, nothing to load
		return "", nil
	}

	// Find the newest snapshot file
	latest := files[0]
	for _, file := range files {
		if file > latest {
			latest = file
		}
	}

	return latest, nil
}

// maintainBackupLimit checks the number of snapshot files in the directory and prunes the oldest
// ones if the limit is exceeded.
func (m *Manager) maintainBackupLimit() {
	// List all snapshot files
	files, err := filepath.Glob(filepath.Join(m.dataDir, backupFileGlob))
	if err != nil {
		log.Error().Err(err).Msg("Failed to list snapshot files")
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
		if err = os.Remove(files[i]); err != nil {
			log.Error().Err(err).Msgf("Failed to remove old snapshot %s:\n", files[i])
		} else {
			log.Debug().Msgf("Pruned old snapshot: %s\n", files[i])
		}
	}
}
