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

	// Clean up empty rows before returning
	for rowKey, rowFamilies := range snap.SnapshotData {

		if rowFamilies == nil || len(*rowFamilies) == 0 {
			fmt.Printf("row %s has no families, removing from snapshot\n", rowKey)
			delete(snap.SnapshotData, rowKey)
			continue
		}

		// Check if any families have qualifiers with data
		isEmpty := true
		for _, qualifiers := range *rowFamilies {
			if len(qualifiers) > 0 {
				isEmpty = false
				break
			}
		}

		// Remove row if it's empty
		if isEmpty {
			delete(snap.SnapshotData, rowKey)
		}
	}

	return snap
}

// snapshotMerge takes all the snapshot data in the files and merges them into the main data
// directory in chronological order (the oldest file to the newest).
//
// Data backup is eventually consistent and works with the reaper GC to keep data in sync.
func (m *Manager) snapshotMerge() error {
	start := time.Now()
	snapshotFiles, err := filepath.Glob(filepath.Join(m.snapshotDir, snapshotFileGlob))
	if err != nil {
		return fmt.Errorf("failed to list incremental snapshot files: %w", err)
	}
	if len(snapshotFiles) == 0 {
		log.Debug().Msg("no incremental snapshots to merge")
		return nil
	}
	sort.Strings(snapshotFiles)
	mergedData := make(litetable.Data)

	// Merge incremental snapshots
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
			continue
		}

		for rowKey, columnFamilies := range snapshot.SnapshotData {
			if columnFamilies == nil {
				continue
			}
			if _, exists := mergedData[rowKey]; !exists {
				mergedData[rowKey] = make(map[string]litetable.VersionedQualifier)
			}
			for familyName, qualifiers := range *columnFamilies {
				if _, exists := mergedData[rowKey][familyName]; !exists {
					mergedData[rowKey][familyName] = make(litetable.VersionedQualifier)
				}
				for qualifier, values := range qualifiers {
					mergedData[rowKey][familyName][qualifier] = values
				}
			}
		}
	}

	latestBackup, err := m.getLatestBackup()
	if err != nil {
		return fmt.Errorf("failed to get latest backup: %w", err)
	}

	// No prior backup, save new
	if latestBackup == "" {
		m.processTombstones(&mergedData)
		cleanupEmptyRows(&mergedData)
		return m.saveBackup(&mergedData)
	}

	// Merge with existing backup
	dataBytes, err := os.ReadFile(latestBackup)
	if err != nil {
		return fmt.Errorf("failed to read snapshot %s: %w", latestBackup, err)
	}

	var loadedData litetable.Data
	if err = json.Unmarshal(dataBytes, &loadedData); err != nil {
		return fmt.Errorf("failed to parse snapshot %s: %w", latestBackup, err)
	}

	// Merge mergedData into loadedData
	for rowKey, columnFamilies := range mergedData {
		if columnFamilies == nil {
			continue
		}
		if _, exists := loadedData[rowKey]; !exists {
			loadedData[rowKey] = make(map[string]litetable.VersionedQualifier)
		}
		for familyName, qualifiers := range columnFamilies {
			if _, exists := loadedData[rowKey][familyName]; !exists {
				loadedData[rowKey][familyName] = make(litetable.VersionedQualifier)
			}
			for qualifier, newValues := range qualifiers {
				// Merge by timestamp
				existing := loadedData[rowKey][familyName][qualifier]
				timestampMap := make(map[int64]litetable.TimestampedValue)
				for _, v := range existing {
					timestampMap[v.Timestamp] = v
				}
				for _, v := range newValues {
					timestampMap[v.Timestamp] = v
				}
				var merged []litetable.TimestampedValue
				for _, v := range timestampMap {
					merged = append(merged, v)
				}
				sort.Slice(merged, func(i, j int) bool {
					return merged[i].Timestamp > merged[j].Timestamp
				})
				loadedData[rowKey][familyName][qualifier] = merged
			}
		}
	}

	// Process tombstones and clean empty structures
	m.processTombstones(&loadedData)
	cleanupEmptyRows(&loadedData)

	// Save new backup
	if err = m.saveBackup(&loadedData); err != nil {
		return fmt.Errorf("failed to save backup: %w", err)
	}

	// Clean up snapshot files
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
					if values[0].ExpiresAt < now {
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

func cleanupEmptyRows(data *litetable.Data) {
	for rowKey, families := range *data {
		for familyName, qualifiers := range families {
			for qualifier, values := range qualifiers {
				if len(values) == 0 {
					delete(qualifiers, qualifier)
				}
			}
			if len(qualifiers) == 0 {
				delete(families, familyName)
			}
		}
		if len(families) == 0 {
			delete(*data, rowKey)
		}
	}
}
