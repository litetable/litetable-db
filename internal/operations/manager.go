package operations

import (
	"errors"
	"github.com/litetable/litetable-db/internal/cdc_emitter"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/internal/shard_storage/wal"
	"time"
)

//go:generate mockgen -destination=manager_mock.go -package=operations -source=manager.go

type writeAhead interface {
	Apply(e *wal.Entry) error
}

type shardManager interface {
	GetRowByFamily(key, family string) (*litetable.Data, bool)
	FilterRowsByPrefix(prefix string) (*litetable.Data, bool)
	FilterRowsByRegex(regex string) (*litetable.Data, bool)

	IsFamilyAllowed(family string) bool
	UpdateFamilies(families []string) error

	Apply(rowKey, family string, qualifiers []string, values [][]byte, timestamp time.Time, expiresAt *time.Time) error
	Delete(key, family string, qualifiers []string, timestamp time.Time,
		expiresAt *time.Time) error
}

type cdc interface {
	Emit(params *cdc_emitter.CDCParams)
}

type Manager struct {
	writeAhead   writeAhead
	defaultTTL   int64
	shardStorage shardManager
	cdc          cdc
	isHealthy    bool
}

type Config struct {
	WAL          writeAhead
	ShardStorage shardManager
	CDC          cdc
}

func (c *Config) validate() error {
	var errGrp []error
	if c.WAL == nil {
		errGrp = append(errGrp, errors.New("WAL cannot be nil"))
	}

	if c.ShardStorage == nil {
		errGrp = append(errGrp, errors.New("shard storage cannot be nil"))
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
		writeAhead:   cfg.WAL,
		defaultTTL:   3600, // configure default for 1 hour
		shardStorage: cfg.ShardStorage,
		cdc:          cfg.CDC,
		isHealthy:    true,
	}, nil
}
