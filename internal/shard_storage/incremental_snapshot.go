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

type snapShopData struct {
	Version           int                                                 `json:"version"`
	SnapshotTimestamp int64                                               `json:"snapshotTimestamp"`
	IsPartial         bool                                                `json:"isPartial"`
	SnapshotData      map[string]*map[string]litetable.VersionedQualifier `json:"snapshotData"`
}

// runIncrementalSnapshot takes every rowKey and family from the changeRows map
// and retrieves the contents of the row in memory and saves it to the snapshot file.
func (m *Manager) runIncrementalSnapshot() error {
	start := time.Now()
	// skip if nothing to do
	if len(m.changedRows) == 0 {
		log.Debug().Msg("no changes to snapshot")
		return nil
	}

	writtenTime := time.Now().UnixNano()

	log.Info().Msgf("creating incremental snapshot: %d", writtenTime)
	snapshot := m.createIncrementalSnapshotData(writtenTime)
	if snapshot.SnapshotData == nil {
		log.Debug().Msg("no snapshot data to save")
		return nil
	}

	// create a partial snapshot
	filename := filepath.Join(m.snapshotDir, fmt.Sprintf("%s-%d.db", snapshotPrefix, writtenTime))

	// Serialize and save to disk
	dataBytes, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot: %w", err)
	}

	if err = os.WriteFile(filename, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	// back up the snapshot information and reset the changed rows
	m.lastPartialSnapshotTime = writtenTime
	m.latestPartialSnapshotFile = filename
	m.changedRows = make(map[string]map[string]struct{})

	log.Info().Str("duration", time.Since(start).String()).Msgf("Incremental snapshot saved to %s",
		filename)
	return nil
}

// createIncrementalSnapshotData creates a snapshot of the current data in memory
func (m *Manager) createIncrementalSnapshotData(time int64) *snapShopData {
	snap := &snapShopData{
		Version:           1,
		SnapshotTimestamp: time,
		IsPartial:         true,
		SnapshotData:      make(map[string]*map[string]litetable.VersionedQualifier),
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

	// Process each changed row
	for rowKey, changedFamilies := range changedRowsCopy {
		// Determine which shard this row belongs to
		shardIdx := m.getShardIndex(rowKey)

		// Get the shard
		sh := m.shardMap[shardIdx]

		// Lock the shard for reading
		sh.mutex.RLock()

		// Check if the row exists in the shard
		row, ok := sh.data[rowKey]
		if !ok {
			sh.mutex.RUnlock()
			log.Debug().Msgf("row %s does not exist in shard %d", rowKey, shardIdx)
			continue
		}

		// Create a map for this row's families in the snapshot
		rowFamilies := make(map[string]litetable.VersionedQualifier)
		snap.SnapshotData[rowKey] = &rowFamilies

		// Process each family that was marked as changed
		for familyName := range changedFamilies {
			family, exists := row[familyName]
			if !exists {
				log.Debug().Msgf("family %s does not exist in row %s", familyName, rowKey)
				continue
			}

			// Create a map for this family's qualifiers
			rowFamilies[familyName] = make(litetable.VersionedQualifier)

			// Deep copy all qualifiers and their values for this family
			for qualifier, values := range family {
				// Make a copy of the values slice
				valuesCopy := make([]litetable.TimestampedValue, len(values))
				copy(valuesCopy, values)

				// Add to the snapshot
				rowFamilies[familyName][qualifier] = valuesCopy
			}
		}

		// Release the shard lock
		sh.mutex.RUnlock()
	}

	return snap
}

// snapshotMerge takes all the snapshot data in the files and merges them into the main data
// directory in chronological order (the oldest file to the newest).
//
// Data backup is eventually consistent and works with the reaper GC to keep data in sync.
func (m *Manager) snapshotMerge() error {
	start := time.Now()
	// Find all incremental snapshot files
	snapshotFiles, err := filepath.Glob(filepath.Join(m.snapshotDir, snapshotFileGlob))
	if err != nil {
		return fmt.Errorf("failed to list incremental snapshot files: %w", err)
	}

	if len(snapshotFiles) == 0 {
		log.Debug().Msg("no incremental snapshots to merge")
		return nil
	}

	// Sort files chronologically
	sort.Strings(snapshotFiles)
	mergedData := make(litetable.Data)

	for _, file := range snapshotFiles {
		fileData, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read snapshot file %s: %w", file, err)
		}
		var snapshot snapShopData
		if err := json.Unmarshal(fileData, &snapshot); err != nil {
			return fmt.Errorf("failed to unmarshal snapshot file %s: %w", file, err)
		}
		if snapshot.SnapshotData == nil {
			log.Debug().Msg("no snapshot data to merge")
			continue
		}

		// Convert from snapshot format to litetable.Data format
		for rowKey, columnFamilies := range snapshot.SnapshotData {
			if columnFamilies == nil {
				continue
			}

			// Initialize row if it doesn't exist
			if _, exists := mergedData[rowKey]; !exists {
				mergedData[rowKey] = make(map[string]litetable.VersionedQualifier)
			}

			for familyName, qualifiers := range *columnFamilies {
				// Check if the qualifier already exists in the family
				if _, exists := mergedData[rowKey][familyName]; !exists {
					mergedData[rowKey][familyName] = make(litetable.VersionedQualifier)
				}
				// Merge the qualifiers into the main data
				for qualifier, values := range qualifiers {
					mergedData[rowKey][familyName][qualifier] = values
				}
			}
		}
	}

	// once we have our backup data, we should get the latest backup (if it exists)
	latestBackup, err := m.getLatestBackup()
	if err != nil {
		return fmt.Errorf("failed to get latest backup: %w", err)
	}

	// its possible the backup hasn't been made yet, just save the data
	if latestBackup == "" {
		// Process tombstones before saving the initial backup
		m.processTombstones(&mergedData)
		err = m.saveBackup(&mergedData)
		if err != nil {
			return fmt.Errorf("failed to save backup: %w", err)
		}
		return nil
	}

	dataBytes, err := os.ReadFile(latestBackup)
	if err != nil {
		return fmt.Errorf("failed to read snapshot %s: %w", latestBackup, err)
	}

	var loadedData litetable.Data
	if err = json.Unmarshal(dataBytes, &loadedData); err != nil {
		return fmt.Errorf("failed to parse snapshot %s: %w", latestBackup, err)
	}

	for rowKey, columnFamilies := range mergedData {
		if columnFamilies == nil {
			continue
		}

		// Initialize row if it doesn't exist
		if _, exists := loadedData[rowKey]; !exists {
			loadedData[rowKey] = make(map[string]litetable.VersionedQualifier)
		}

		for familyName, qualifiers := range columnFamilies {
			if qualifiers == nil {
				continue
			}

			// Check if the family already exists
			if _, exists := loadedData[rowKey][familyName]; !exists {
				loadedData[rowKey][familyName] = make(litetable.VersionedQualifier)
			}

			// Merge the qualifiers into the main data
			for qualifier, values := range qualifiers {
				// Instead of directly copying values, merge them properly
				if existingValues, exists := loadedData[rowKey][familyName][qualifier]; exists {
					// Create a map to track values by timestamp to avoid duplicates
					valuesByTimestamp := make(map[int64]litetable.TimestampedValue)

					// Add existing values to the map
					for _, val := range existingValues {
						valuesByTimestamp[val.Timestamp] = val
					}

					// Add or override with new values
					for _, val := range values {
						valuesByTimestamp[val.Timestamp] = val
					}

					// Convert back to slice
					var mergedValues []litetable.TimestampedValue
					for _, val := range valuesByTimestamp {
						mergedValues = append(mergedValues, val)
					}

					// Sort by timestamp (newest first)
					sort.Slice(mergedValues, func(i, j int) bool {
						return mergedValues[i].Timestamp > mergedValues[j].Timestamp
					})

					loadedData[rowKey][familyName][qualifier] = mergedValues
				} else {
					// No existing values, just copy the new ones
					loadedData[rowKey][familyName][qualifier] = values
				}
			}
		}
	}

	// Process tombstones to remove tombstoned data
	m.processTombstones(&loadedData)

	// save the backup file
	err = m.saveBackup(&loadedData)
	if err != nil {
		return fmt.Errorf("failed to save backup: %w", err)
	}

	// once we know the backup is saved, purge the snapshot files
	// Delete the oldest files, keeping only the configured limit
	for _, file := range snapshotFiles {
		if err = os.Remove(file); err != nil {
			log.Error().Err(err).Msgf("Failed to remove old snapshot: %s", file)
		} else {
			log.Debug().Msgf("Pruned old snapshot: %s", file)
		}
	}

	log.Debug().Str("duration", time.Since(start).String()).Msg("incremental snapshot merge complete")
	return nil
}

// processTombstones processes the data and removes tombstoned entries
func (m *Manager) processTombstones(data *litetable.Data) {
	now := time.Now().UnixNano()

	for rowKey, columnFamilies := range *data {
		for familyName, qualifiers := range columnFamilies {
			// Process each qualifier
			for qualifierName, values := range qualifiers {
				if len(values) == 0 {
					// Remove empty qualifier lists
					delete(qualifiers, qualifierName)
					continue
				}

				// The values are sorted by timestamp (newest first)
				// If the newest value is a tombstone, we should remove all values for this qualifier
				// or keep it only if the tombstone hasn't expired yet
				if len(values) > 0 && values[0].IsTombstone {
					// If the tombstone is expired, remove the entire qualifier
					if *values[0].ExpiresAt < now {
						delete(qualifiers, qualifierName)
					}
				}
			}

			// Clean up empty families
			if len(qualifiers) == 0 {
				delete(columnFamilies, familyName)
			}
		}

		// Clean up empty rows
		if len(columnFamilies) == 0 {
			delete(*data, rowKey)
		}
	}
}
