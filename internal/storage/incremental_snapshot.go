package storage

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
	SnapshotTimestamp time.Time                                           `json:"snapshotTimestamp"`
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

	writtenTime := time.Now()

	log.Info().Msg("creating incremental snapshot: " + writtenTime.String())
	snapshot := m.createIncrementalSnapshotData(writtenTime)
	if snapshot.SnapshotData == nil {
		log.Debug().Msg("no snapshot data to save")
		return nil
	}

	// create a partial snapshot
	filename := filepath.Join(m.snapshotDir, fmt.Sprintf("%s-%d.db", snapshotPrefix, writtenTime.UnixNano()))

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
func (m *Manager) createIncrementalSnapshotData(time time.Time) *snapShopData {
	snap := &snapShopData{
		Version:           1,
		SnapshotTimestamp: time,
		IsPartial:         true,
		SnapshotData:      make(map[string]*map[string]litetable.VersionedQualifier),
	}

	// lock the mutex around working with the cache
	m.mutex.Lock()
	defer m.mutex.Unlock()

	data := m.data

	for k, cf := range m.changedRows {
		// check to see if the key exists in memory if it doesn't just continue on
		row, ok := data[k]
		if !ok {
			fmt.Printf("row %s does not exist in data\n", k)
			continue
		}

		rowFamilies := make(map[string]litetable.VersionedQualifier)
		snap.SnapshotData[k] = &rowFamilies

		// check to see if the family exists in on the row if it doesn't just continue on
		for fam := range cf {
			family, exists := row[fam]
			if !exists {
				fmt.Printf("family %s does not exist in row %s\n", fam, k)
				continue
			}

			rowFamilies[fam] = make(litetable.VersionedQualifier)

			// Copy each qualifier from this family to the versioned qualifier
			for qualifier, values := range family {
				rowFamilies[fam][qualifier] = values
			}
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

			// Check if the qualifier already exists in the family
			if _, exists := loadedData[rowKey][familyName]; !exists {
				loadedData[rowKey][familyName] = make(litetable.VersionedQualifier)
			}
			// Merge the qualifiers into the main data
			for qualifier, values := range qualifiers {
				loadedData[rowKey][familyName][qualifier] = values
			}
		}
	}

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
