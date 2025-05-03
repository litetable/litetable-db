package wal

import (
	"encoding/json"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Parallel()
	t.Run("Invalid config", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{}

		got, err := New(cfg)
		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("Valid config", func(t *testing.T) {
		t.Parallel()
		dir, _ := os.Getwd()
		testDir := filepath.Join(dir, defaultWalDirectory)
		cfg := &Config{
			Path: testDir,
		}
		got, err := New(cfg)
		require.NoError(t, err)
		require.NotNil(t, got)
	})
}

func TestManager_Apply(t *testing.T) {
	t.Parallel()
	t.Run("Valid entry", func(t *testing.T) {
		t.Parallel()
		dir, _ := os.Getwd()
		testDir := filepath.Join(dir, ".temp-test")
		cfg := &Config{
			Path: testDir,
		}

		m, err := New(cfg)
		require.NoError(t, err)
		now := time.Now()

		entry := &Entry{
			Operation: litetable.OperationWrite,
			Query:     []byte("test query"),
			Timestamp: now,
		}

		applyErr := m.Apply(entry)
		require.NoError(t, applyErr)

		// Check if the entry was written to the WAL file
		file, err := os.Open(m.walFile.Name())
		require.NoError(t, err)
		defer file.Close()

		t.Log(file.Name())
		stat, err := file.Stat()
		require.NoError(t, err)
		require.Greater(t, stat.Size(), int64(0), "WAL file should not be empty")

		// wal should have example 1 entry
		// read the file and check if the entry is present
		fileContent := make([]byte, stat.Size())
		_, err = file.Read(fileContent)
		require.NoError(t, err)

		// try to unmarshal the entry
		var entryRead Entry
		err = json.Unmarshal(fileContent, &entryRead)
		require.NoError(t, err)
		require.Equal(t, entry.Operation, entryRead.Operation)
		require.Equal(t, string(entry.Query), string(entryRead.Query))
		require.Equal(t, entry.Timestamp.Unix(), entryRead.Timestamp.Unix())

		// remove the test directory
		err = os.RemoveAll(testDir)
		require.NoError(t, err)
	})
}
