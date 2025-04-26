package engine

import (
	"db/internal/litetable"
	"errors"
	"fmt"
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
}

type Config struct {
	WAL wal
}

func (c *Config) validate() error {
	var errGrp []error
	if c.WAL == nil {
		errGrp = append(errGrp, fmt.Errorf("WAL is required"))
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
	}, nil
}

// Start replays the WAL to the LiteTable in-memory data source.
func (e *Engine) Start() error {
	if err := e.wal.Load(e.data); err != nil {
		return fmt.Errorf("failed to load WAL: %w", err)
	}

	return nil
}

func (e *Engine) Stop() error {
	return nil
}

func (e *Engine) Name() string {
	return "LiteTable Engine"
}
