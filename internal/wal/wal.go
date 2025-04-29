package wal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultWalDirectory = "wal"
	defaultWALFile      = "wal.log"
)

// Entry represents a Write-Ahead Log entry for a database operation
type Entry struct {
	Protocol  int       `json:"protocol"`
	Query     []byte    `json:"query"`
	Timestamp time.Time `json:"timestamp"`
}

type Manager struct {
	mu      sync.RWMutex
	walFile *os.File
	path    string
}

type Config struct {
	// Path where the WAL directory will be saved
	Path string
}

func (c *Config) validate() error {
	var errGrp []error
	if c.Path == "" {
		errGrp = append(errGrp, errors.New("home directory cannot be empty"))
	}
	// Path is optional, so no validation needed
	return errors.Join(errGrp...)
}

func New(cfg *Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	walPath := filepath.Join(cfg.Path, defaultWalDirectory, defaultWALFile)
	walDir := filepath.Dir(walPath)
	if err := os.MkdirAll(walDir, 0750); err != nil {
		return nil, errors.New("failed to create WAL directory: " + err.Error())
	}

	// Open WAL file with appropriate permissions
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		return nil, errors.New("failed to open WAL file: " + err.Error())
	}

	return &Manager{
		walFile: file,
		path:    walPath,
	}, nil
}

// Apply takes in the query bytes and appends to the WAL file:
//
// ex: key=testKey:12345 family=main qualifier=status value=active qualifier=time value=now
//
// WAL is written locally to allow replaying the log in case of failure. If the WAL is lost
// or corrupted, the data is still available in the database. The WAL is used to ensure
// that the data is written to the database before the transaction is considered complete.
func (m *Manager) Apply(e *Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Convert the entry to JSON for storage
	jsonData, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	// Write the JSON data to the WAL file, followed by a newline
	if _, err = m.walFile.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	return nil
}

// filePath returns the location of the WAL file
func (m *Manager) filePath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", errors.New("failed to get home directory: " + err.Error())
	}
	walPath := filepath.Join(homeDir, ".litetable/wal", defaultWALFile)

	return walPath, nil
}
