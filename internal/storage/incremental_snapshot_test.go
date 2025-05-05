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

	m := &Manager{
		data: map[string]map[string]litetable.VersionedQualifier{
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
		},
		snapshotDir: t.TempDir(),
	}

	m.MarkRowChanged("family1", "row1")
	m.MarkRowChanged("family2", "row1")
	m.MarkRowChanged("family1", "row2")

	err := m.runIncrementalSnapshot()

	req := require.New(t)
	req.NoError(err, "should not return an error")
	req.NotEqual("", m.latestSnapshotFile, "should have a snapshot file")
	req.True(m.lastSnapshotTime.After(time.Time{}), "should have a valid timestamp")

	// let's validate the data that was written
	tmpFile, err := os.ReadFile(m.latestSnapshotFile)
	req.NoError(err, "should not return an error")
	req.NotEmpty(tmpFile, "should have data in the file")

	var snapshot snapShopData
	err = json.Unmarshal(tmpFile, &snapshot)
	req.NoError(err, "should not return an error")

	req.Equal(1, snapshot.Version, "should have the correct version")
	req.Equal(m.lastSnapshotTime.Unix(), snapshot.SnapshotTimestamp.Unix(), "should have the correct timestamp")
	req.Equal(2, len(snapshot.SnapshotData), "should have 2 rows")

	req.Equal(2, len(*snapshot.SnapshotData["row1"]), "should have 2 families")

	req.Equal(1, len(*snapshot.SnapshotData["row2"]), "should have 1 families")
}
