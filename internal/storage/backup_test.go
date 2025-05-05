package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestMaintainSnapshotLimit(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	// Create a test manager with a limit of 3 snapshots
	manager := &Manager{
		dataDir:          tempDir,
		maxSnapshotLimit: 3,
	}

	// Create 5 snapshot files with timestamps precisely 1 month apart
	ts1 := time.Date(2023, 1, 15, 12, 0, 0, 0, time.UTC).UnixNano() // Jan 15, 2023 (oldest)
	ts2 := time.Date(2023, 2, 15, 12, 0, 0, 0, time.UTC).UnixNano() // Feb 15, 2023
	ts3 := time.Date(2023, 3, 15, 12, 0, 0, 0, time.UTC).UnixNano() // Mar 15, 2023
	ts4 := time.Date(2023, 4, 15, 12, 0, 0, 0, time.UTC).UnixNano() // Apr 15, 2023
	ts5 := time.Date(2023, 5, 15, 12, 0, 0, 0, time.UTC).UnixNano() // May 15, 2023 (newest)

	// Add timestamps in non-sequential order to test sorting
	timestamps := []int64{ts3, ts1, ts5, ts2, ts4}
	// The 3 newest timestamps that should be kept
	expectedTimestamps := []int64{ts3, ts4, ts5}

	// Create the snapshot files
	for _, ts := range timestamps {
		filename := filepath.Join(tempDir, fmt.Sprintf("backup-%d.db", ts))
		require.NoError(t, os.WriteFile(filename, []byte{}, 0644))
	}

	// Get initial files
	initialFiles, err := filepath.Glob(filepath.Join(tempDir, backupFileGlob))
	require.NoError(t, err)
	assert.Len(t, initialFiles, 5, "Should have 5 snapshot files initially")

	// Run the function to maintain snapshot limit
	manager.maintainSnapshotLimit()

	// Check remaining files
	remainingFiles, err := filepath.Glob(filepath.Join(tempDir, backupFileGlob))
	require.NoError(t, err)
	assert.Len(t, remainingFiles, 3, "Should have pruned to 3 snapshot files")

	// Extract timestamps from remaining files
	var keptTimestamps []int64
	for _, file := range remainingFiles {
		ts := extractTimestamp(filepath.Base(file))
		keptTimestamps = append(keptTimestamps, ts)
	}
	sort.Slice(keptTimestamps, func(i, j int) bool { return keptTimestamps[i] < keptTimestamps[j] })

	sort.Slice(expectedTimestamps, func(i, j int) bool { return expectedTimestamps[i] < expectedTimestamps[j] })

	// Verify we kept exactly the expected timestamps
	assert.Equal(t, expectedTimestamps, keptTimestamps, "The three newest snapshots should be kept")

	// Verify the two oldest were deleted
	for _, ts := range keptTimestamps {
		assert.NotEqual(t, ts1, ts, "Oldest snapshot (Jan) should have been deleted")
		assert.NotEqual(t, ts2, ts, "Second oldest snapshot (Feb) should have been deleted")
	}
}

// Helper function to extract timestamp from filename
func extractTimestamp(filename string) int64 {
	// Extract the numeric part between "snapshot-" and ".db"
	var timestamp int64
	_, err := fmt.Sscanf(filename, "backup-%d.db", &timestamp)
	if err != nil {
		return 0
	}
	return timestamp
}
