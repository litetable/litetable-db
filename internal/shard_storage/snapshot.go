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

const (
	snapshotPrefix   = "ss-incr"
	snapshotFileGlob = "ss-incr-*.db"
)

// directSnapshotData represents the structure of our simplified snapshot format
type directSnapshotData struct {
	Version           int                                                `json:"version"`
	SnapshotTimestamp int64                                              `json:"snapshotTimestamp"`
	SnapshotData      map[string]map[string]litetable.VersionedQualifier `json:"snapshotData"`
}

// createDirectSnapshot creates a new snapshot of changed rows directly from memory
// without any complex merging logic
func (m *Manager) createDirectSnapshot() error {
	start := time.Now()

	// Skip if nothing to do
	if len(m.changedRows) == 0 {
		log.Debug().Msg("no changes to snapshot")
		return nil
	}

	snapshotTime := time.Now().UnixNano()
	log.Info().Msgf("creating direct snapshot: %d", snapshotTime)

	// Create snapshot data
	snapshot := &directSnapshotData{
		Version:           1,
		SnapshotTimestamp: snapshotTime,
		SnapshotData:      make(map[string]map[string]litetable.VersionedQualifier),
	}

	// Lock the mutex around working with the changed rows
	m.mutex.RLock()
	// Make a copy of changed rows so we can release the lock quickly
	changedRowsCopy := make(map[string]map[string]struct{}, len(m.changedRows))
	for rowKey, families := range m.changedRows {
		familiesCopy := make(map[string]struct{}, len(families))
		for family := range families {
			familiesCopy[family] = struct{}{}
		}
		changedRowsCopy[rowKey] = familiesCopy
	}
	m.mutex.RUnlock()

	// Process each changed row by doing a direct copy from memory
	for rowKey, changedFamilies := range changedRowsCopy {
		// Determine which shard this row belongs to
		shardIdx := m.getShardIndex(rowKey)
		sh := m.shardMap[shardIdx]

		// Lock the shard for reading
		sh.mutex.RLock()

		// Check if the row exists in the shard
		row, exists := sh.data[rowKey]
		if !exists {
			// If the row doesn't exist in memory but was marked as changed,
			// we need to ensure it's deleted from the backup too
			sh.mutex.RUnlock()
			snapshot.SnapshotData[rowKey] = nil // null marker indicates deletion
			log.Debug().Msgf("row %s marked as deleted in snapshot", rowKey)
			continue
		}

		// Create a deep copy of the row data
		snapshotRow := make(map[string]litetable.VersionedQualifier)

		for familyName := range changedFamilies {
			family, exists := row[familyName]
			if !exists {
				// Family doesn't exist but was marked as changed - it was deleted
				snapshotRow[familyName] = nil
				log.Debug().Msgf("family %s marked as deleted in row %s", familyName, rowKey)
				continue
			}

			// Deep copy the family data
			familyCopy := make(litetable.VersionedQualifier)
			for qualifier, values := range family {
				// Skip tombstone qualifiers when their expiration time has passed,
				// This is cleanup for any qualifier that is deleted. We want to make sure to
				// reclaim that space in the backup.
				if len(values) > 0 && values[0].IsTombstone && values[0].ExpiresAt <= time.Now().
					UnixNano() {
					continue
				}

				// Deep copy the values
				valuesCopy := make([]litetable.TimestampedValue, len(values))
				copy(valuesCopy, values)
				familyCopy[qualifier] = valuesCopy
			}

			snapshotRow[familyName] = familyCopy
		}

		snapshot.SnapshotData[rowKey] = snapshotRow
		sh.mutex.RUnlock()
	}

	// Serialize and save to disk
	filename := filepath.Join(m.snapshotDir, fmt.Sprintf("%s-%d.db", snapshotPrefix, snapshotTime))
	dataBytes, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to serialize direct snapshot: %w", err)
	}

	if err = os.WriteFile(filename, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write direct snapshot file: %w", err)
	}

	// Reset changed rows tracking
	m.mutex.Lock()
	m.changedRows = make(map[string]map[string]struct{})
	m.mutex.Unlock()

	log.Info().Str("duration", time.Since(start).String()).Msgf("Direct snapshot saved to %s", filename)
	return nil
}

// ApplyDirectSnapshots applies all direct snapshots to the main backup file
func (m *Manager) ApplyDirectSnapshots() error {
	start := time.Now()

	// Find all snapshot files
	snapshotFiles, err := filepath.Glob(filepath.Join(m.snapshotDir, snapshotFileGlob))
	if err != nil {
		return fmt.Errorf("failed to list direct snapshot files: %w", err)
	}

	if len(snapshotFiles) == 0 {
		log.Debug().Msg("no direct snapshots to apply")
		return nil
	}

	// Sort files by name (which includes timestamp) to process in order
	sort.Strings(snapshotFiles)

	// Load current backup
	backup, err := m.loadLatestBackup()
	if err != nil {
		log.Warn().Err(err).Msg("couldn't load existing backup, starting fresh")
		backup = make(litetable.Data)
	}

	// Apply each snapshot in order
	snapshotsApplied := 0
	rowsModified := 0

	for _, file := range snapshotFiles {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read snapshot %s: %w", file, err)
		}

		var snapshot directSnapshotData
		if err := json.Unmarshal(data, &snapshot); err != nil {
			return fmt.Errorf("failed to parse snapshot %s: %w", file, err)
		}

		// Apply changes from this snapshot
		for rowKey, rowData := range snapshot.SnapshotData {
			if rowData == nil {
				// Explicit deletion marker
				delete(backup, rowKey)
				rowsModified++
				log.Debug().Msgf("deleted row %s from backup", rowKey)
				continue
			}

			// Update or create row
			if _, exists := backup[rowKey]; !exists {
				backup[rowKey] = make(map[string]litetable.VersionedQualifier)
			}

			for familyName, qualifiers := range rowData {
				if qualifiers == nil {
					// Family deletion marker
					delete(backup[rowKey], familyName)
					log.Debug().Msgf("deleted family %s from row %s in backup", familyName, rowKey)
				} else {
					// Replace family data with snapshot data
					backup[rowKey][familyName] = qualifiers
				}
			}

			// Clean up empty row if needed
			if len(backup[rowKey]) == 0 {
				delete(backup, rowKey)
				log.Debug().Msgf("row %s became empty and was removed from backup", rowKey)
			}

			rowsModified++
		}

		snapshotsApplied++
	}

	// Save updated backup
	if err := m.saveBackup(&backup); err != nil {
		return fmt.Errorf("failed to save backup after applying snapshots: %w", err)
	}

	// Clean up processed snapshot files
	for _, file := range snapshotFiles {
		if err := os.Remove(file); err != nil {
			log.Error().Err(err).Msgf("failed to remove processed snapshot: %s", file)
		}
	}

	log.Info().
		Str("duration", time.Since(start).String()).
		Int("snapshots_applied", snapshotsApplied).
		Int("rows_modified", rowsModified).
		Msg("applied direct snapshots to backup")

	return nil
}
