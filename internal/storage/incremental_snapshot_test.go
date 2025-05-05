package storage

import (
	"encoding/json"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestManager_runIncrementalSnapshot(t *testing.T) {

	mockData := map[string]map[string]litetable.VersionedQualifier{
		"row1": {
			"family1": litetable.VersionedQualifier{
				"qualifier1": []litetable.TimestampedValue{
					{Value: []byte("value1"), Timestamp: time.Now()},
				},
			},
			"family2": litetable.VersionedQualifier{
				"qualifier2": []litetable.TimestampedValue{
					{Value: []byte("value1"), Timestamp: time.Now()},
				},
			},
			"family3": litetable.VersionedQualifier{
				"qualifier5": []litetable.TimestampedValue{
					{Value: []byte("value1"), Timestamp: time.Now()},
				},
			},
		},
		"row2": {
			"family1": litetable.VersionedQualifier{
				"qualifier4": []litetable.TimestampedValue{
					{Value: []byte("value1"), Timestamp: time.Now()},
				},
			},
		},
	}
	tests := map[string]struct {
		manager     *Manager
		shouldWrite bool
	}{
		"successful run without changes": {
			manager: &Manager{
				snapshotDir: t.TempDir(),
			},
		},
		"successful run with changes": {
			manager: &Manager{
				snapshotDir: t.TempDir(),
				data:        mockData,
			},
			shouldWrite: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.shouldWrite {
				tc.manager.MarkRowChanged("family1", "row1")
				tc.manager.MarkRowChanged("family2", "row1")
				tc.manager.MarkRowChanged("family1", "row2")

				err := tc.manager.runIncrementalSnapshot()

				req := require.New(t)
				req.NoError(err, "should not return an error")
				req.NotEqual("", tc.manager.latestPartialSnapshotFile, "should have a snapshot file")
				req.True(tc.manager.lastPartialSnapshotTime.After(time.Time{}), "should have a valid timestamp")

				// let's validate the data that was written
				tmpFile, err := os.ReadFile(tc.manager.latestPartialSnapshotFile)
				req.NoError(err, "should not return an error")
				req.NotEmpty(tmpFile, "should have data in the file")

				var snapshot snapShopData
				err = json.Unmarshal(tmpFile, &snapshot)
				req.NoError(err, "should not return an error")

				req.Equal(1, snapshot.Version, "should have the correct version")
				req.Equal(tc.manager.lastPartialSnapshotTime.Unix(), snapshot.SnapshotTimestamp.Unix(), "should have the correct timestamp")
				req.Equal(2, len(snapshot.SnapshotData), "should have 2 rows")

				req.Equal(2, len(*snapshot.SnapshotData["row1"]), "should have 2 families")

				req.Equal(1, len(*snapshot.SnapshotData["row2"]), "should have 1 families")
			} else {
				// run the snapshot without any changes
				err := tc.manager.runIncrementalSnapshot()

				req := require.New(t)
				req.NoError(err, "should not return an error")
				req.Equal("", tc.manager.latestPartialSnapshotFile, "should not have a snapshot file")
				req.Equal(time.Time{}, tc.manager.lastPartialSnapshotTime, "should not have a valid timestamp")
			}
		})
	}
}
