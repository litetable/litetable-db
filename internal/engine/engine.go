package engine

import (
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/storage"
	"sync"
)

type wal interface {
	Apply(msgType int, query []byte) error
	Load(source map[string]map[string]litetable.VersionedQualifier) error
}

// Engine is the main struct that provides the interface to the LiteTable server.
type Engine struct {
	rwMutex       sync.RWMutex
	data          map[string]map[string]litetable.VersionedQualifier // rowKey -> family -> qualifier -> []TimestampedValue
	maxBufferSize int
	wal           wal
	storage       *storage.Disk
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
	return &Engine{
		rwMutex:       sync.RWMutex{},
		maxBufferSize: 4096,
		wal:           cfg.WAL,
		data:          make(map[string]map[string]litetable.VersionedQualifier),
		storage:       cfg.Storage,
	}, nil
}

// Start replays the WAL to the LiteTable in-memory data source.
func (e *Engine) Start() error {
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

	if err := e.wal.Load(e.data); err != nil {
		return fmt.Errorf("failed to load WAL: %w", err)
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
