package engine

import (
	"errors"
	"fmt"
	wal2 "github.com/litetable/litetable-db/internal/wal"
	"sync"
)

type protocolManager interface {
	RunOperation(buf []byte) ([]byte, error)
}

type wal interface {
	Apply(e *wal2.Entry) error
}

// Engine is the main struct that provides the interface to the LiteTable server and holds all the
// data in memory. It is responsible for orchestrating the LiteTable protocol.
type Engine struct {
	rwMutex       sync.RWMutex
	maxBufferSize int
	wal           wal
	protocol      protocolManager
}

type Config struct {
	WAL           wal
	Protocol      protocolManager
	MaxBufferSize int
}

func (c *Config) validate() error {
	var errGrp []error
	if c.WAL == nil {
		errGrp = append(errGrp, fmt.Errorf("WAL is required"))
	}

	if c.Protocol == nil {
		errGrp = append(errGrp, fmt.Errorf("protocol is required"))
	}
	return errors.Join(errGrp...)
}

func New(cfg *Config) (*Engine, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	e := &Engine{
		rwMutex:       sync.RWMutex{},
		maxBufferSize: 4096,
		wal:           cfg.WAL,
		protocol:      cfg.Protocol,
	}

	if cfg.MaxBufferSize > 0 {
		e.maxBufferSize = cfg.MaxBufferSize
	}
	return e, nil
}

// Start loads the data into memory.
func (e *Engine) Start() error {
	return nil
}

func (e *Engine) Stop() error {
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	return nil
}

func (e *Engine) Name() string {
	return "Litetable Engine"
}
