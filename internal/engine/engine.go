package engine

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

//go:generate mockgen -destination=engine_mock.go -source=engine.go -package=engine

// Conn is a wrapper around net.Conn to allow for mocking
type Conn interface {
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close() error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

type ops interface {
	Run(buf []byte) ([]byte, error)
}

// Engine is the main struct that provides the interface to the LiteTable server and holds all the
// data in memory. It is responsible for orchestrating the LiteTable protocol.
type Engine struct {
	rwMutex       sync.RWMutex
	maxBufferSize int
	operations    ops
}

type Config struct {
	OperationManager ops
	MaxBufferSize    int
}

func (c *Config) validate() error {
	var errGrp []error

	if c.OperationManager == nil {
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
		operations:    cfg.OperationManager,
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

// Stop ensures the engine does not shutdown till all currently accepted operations are completed.
// TODO: actually wait for all operations to finish
func (e *Engine) Stop() error {
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	return nil
}

func (e *Engine) Name() string {
	return "LiteTable Engine"
}
