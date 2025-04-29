package protocol

import (
	"errors"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/reaper"
)

type garbageCollector interface {
	Reap(p *reaper.GCParams)
}

type Manager struct {
	garbageCollector garbageCollector
	defaultTTL       int64
}

type Config struct {
	GarbageCollector garbageCollector
}

func (c *Config) validate() error {
	var errGrp []error
	if c.GarbageCollector == nil {
		errGrp = append(errGrp, errors.New("garbage collector cannot be nil"))
	}
	return errors.Join(errGrp...)
}

// New creates a new protocol manager
func New(cfg *Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &Manager{
		garbageCollector: cfg.GarbageCollector,
		defaultTTL:       3600, // configure default for 1 hour
	}, nil
}

type ReadParams struct {
	Query              []byte
	Data               *litetable.Data
	ConfiguredFamilies []string
}
