package operations

import (
	"errors"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/reaper"
	"github.com/litetable/litetable-db/internal/wal"
)

type writeAhead interface {
	Apply(e *wal.Entry) error
}

type garbageCollector interface {
	Reap(p *reaper.GCParams)
}

type storageManager interface {
	GetData() *litetable.Data
	IsFamilyAllowed(family string) bool
	UpdateFamilies(families []string) error
}

type Manager struct {
	garbageCollector garbageCollector
	writeAhead       writeAhead
	defaultTTL       int64
	storage          storageManager
}

type Config struct {
	GarbageCollector garbageCollector
	WAL              writeAhead
	Storage          storageManager
}

func (c *Config) validate() error {
	var errGrp []error
	if c.GarbageCollector == nil {
		errGrp = append(errGrp, errors.New("garbage collector cannot be nil"))
	}
	if c.WAL == nil {
		errGrp = append(errGrp, errors.New("WAL cannot be nil"))
	}
	if c.Storage == nil {
		errGrp = append(errGrp, errors.New("storage cannot be nil"))
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
		writeAhead:       cfg.WAL,
		defaultTTL:       3600, // configure default for 1 hour
		storage:          cfg.Storage,
	}, nil
}
