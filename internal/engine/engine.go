package engine

import (
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/protocol"
	"github.com/litetable/litetable-db/internal/storage"
	"sync"
)

type wal interface {
	Apply(msgType int, query []byte) error
	Load(source protocol.DataFormat) error
}

// Engine is the main struct that provides the interface to the LiteTable server.
type Engine struct {
	rwMutex       sync.RWMutex
	data          protocol.DataFormat // rowKey -> family -> qualifier -> []TimestampedValue
	maxBufferSize int
	wal           wal
	storage       *storage.Disk

	allowedFamilies []string // Maps family names to allowed columns
	familiesFile    string   // Path to store allowed families configuration
}

type Config struct {
	WAL     wal
	Storage *storage.Disk
}

func (c *Config) validate() error {
	var errGrp []error
	if c.WAL == nil {
		errGrp = append(errGrp, fmt.Errorf("WAL is required"))
	}

	if c.Storage == nil {
		errGrp = append(errGrp, fmt.Errorf("storage is required"))
	}

	return errors.Join(errGrp...)
}

func New(cfg *Config) (*Engine, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	e := &Engine{
		rwMutex:         sync.RWMutex{},
		maxBufferSize:   4096,
		wal:             cfg.WAL,
		data:            make(protocol.DataFormat),
		storage:         cfg.Storage,
		allowedFamilies: make([]string, 0),
		familiesFile:    cfg.Storage.FamilyLockFile(),
	}

	// Load allowed families from disk
	if err := e.loadAllowedFamilies(); err != nil {
		return nil, err
	}

	return e, nil
}

// Start replays the WAL to the LiteTable in-memory data source.
func (e *Engine) Start() error {
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	// Clear any existing data to prevent duplication
	e.data = make(protocol.DataFormat)

	// First load data from disk storage
	diskData, err := e.storage.LoadFromDisk()
	if err != nil {
		return fmt.Errorf("failed to load data from disk: %w", err)
	}

	// Populate engine's in-memory data from disk storage
	for rowKey, families := range diskData {
		if e.data[rowKey] == nil {
			e.data[rowKey] = make(map[string]litetable.VersionedQualifier)
		}
		for family, qualifier := range families {
			e.data[rowKey][family] = qualifier
		}
	}

	return nil
}

func (e *Engine) Stop() error {

	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	// Flush all data to disk storage before shutting down
	if err := e.storage.ForceFlush(); err != nil {
		return fmt.Errorf("failed to flush data to disk: %w", err)
	}

	return nil
}

func (e *Engine) Name() string {
	return "Litetable Engine"
}

func (e *Engine) Data() *protocol.DataFormat {
	return &e.data
}
