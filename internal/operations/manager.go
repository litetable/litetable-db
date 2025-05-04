package operations

import (
	"errors"
	"github.com/litetable/litetable-db/internal/cdc_emitter"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/reaper"
	"github.com/litetable/litetable-db/internal/wal"
)

//go:generate mockgen -destination=manager_mock.go -package=operations -source=manager.go

type writeAhead interface {
	Apply(e *wal.Entry) error
}

type garbageCollector interface {
	Reap(p *reaper.ReapParams)
}

type storageManager interface {
	GetData() *litetable.Data
	IsFamilyAllowed(family string) bool
	UpdateFamilies(families []string) error
}

type cdc interface {
	Emit(params *cdc_emitter.CDCParams)
}

type Manager struct {
	garbageCollector garbageCollector
	writeAhead       writeAhead
	defaultTTL       int64
	storage          storageManager
	cdc              cdc
	isHealthy        bool
}

type Config struct {
	GarbageCollector garbageCollector
	WAL              writeAhead
	Storage          storageManager
	CDC              cdc
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
	if c.CDC == nil {
		errGrp = append(errGrp, errors.New("CDC emitter cannot be nil"))
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
		cdc:              cfg.CDC,
		isHealthy:        true,
	}, nil
}
