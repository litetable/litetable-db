package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/protocol"
	"github.com/litetable/litetable-db/internal/storage"
	"os"
	"sync"
)

type query interface {
	Read(params *protocol.ReadParams) ([]byte, error)
	Write(params *protocol.WriteParams) ([]byte, error)
	Delete(params *protocol.DeleteParams) error
	Create(params *protocol.CreateParams) error
}

type wal interface {
	Apply(msgType int, query []byte) error
	Load(source protocol.DataFormat) error
}

// Engine is the main struct that provides the interface to the LiteTable server and holds all the
// data in memory. It is responsible for orchestrating the LiteTable protocol.
type Engine struct {
	rwMutex       sync.RWMutex
	data          protocol.DataFormat // rowKey -> family -> qualifier -> []TimestampedValue
	maxBufferSize int
	wal           wal
	storage       *storage.Disk

	allowedFamilies []string // Maps family names to allowed columns
	familiesFile    string   // Path to store allowed families configuration
	protocol        query
}

type Config struct {
	WAL      wal
	Protocol query
	Storage  *storage.Disk
}

func (c *Config) validate() error {
	var errGrp []error
	if c.WAL == nil {
		errGrp = append(errGrp, fmt.Errorf("WAL is required"))
	}

	if c.Storage == nil {
		errGrp = append(errGrp, fmt.Errorf("storage is required"))
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
		rwMutex:         sync.RWMutex{},
		maxBufferSize:   4096,
		wal:             cfg.WAL,
		data:            make(protocol.DataFormat),
		storage:         cfg.Storage,
		allowedFamilies: make([]string, 0),
		familiesFile:    cfg.Storage.FamilyLockFile(),
		protocol:        cfg.Protocol,
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

func (e *Engine) saveAllowedFamilies(families []string) error {
	e.allowedFamilies = families
	data, err := json.Marshal(families)
	if err != nil {
		return fmt.Errorf("failed to marshal allowed families: %w", err)
	}
	return os.WriteFile(e.familiesFile, data, 0644)
}

func (e *Engine) loadAllowedFamilies() error {
	data, err := os.ReadFile(e.familiesFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, not an error
			return nil
		}
		return fmt.Errorf("failed to read allowed families file: %w", err)
	}

	return json.Unmarshal(data, &e.allowedFamilies)
}
