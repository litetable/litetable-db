package protocol

import (
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
)

type dataStorage interface {
	Write(rowKey, family string, qualifier litetable.VersionedQualifier) error
}

type Manager struct {
	dataStorage dataStorage
}

type Config struct {
	Storage dataStorage
}

func (c *Config) validate() error {
	if c.Storage == nil {
		return fmt.Errorf("storage is required")
	}
	return nil
}

// New creates a new protocol manager
func New(cfg *Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Manager{
		dataStorage: cfg.Storage,
	}, nil
}

type ReadParams struct {
	Query              []byte
	Data               *DataFormat
	ConfiguredFamilies []string
}
