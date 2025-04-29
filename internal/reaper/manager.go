package reaper

import (
	"context"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/internal/protocol"
	"sync"
	"time"
)

type storage interface {
	GetData() *protocol.DataFormat
}

type Reaper struct {
	collector chan ReapParams
	storage   storage

	mutex        sync.Mutex
	reapInterval time.Duration

	procCtx context.Context
	cancel  context.CancelFunc
}

type Config struct {
	Storage    storage
	GCInterval int
}

func (c *Config) validate() error {
	var errGrp []error
	if c.Storage == nil {
		errGrp = append(errGrp, errors.New("storage cannot be nil"))
	}
	if c.GCInterval <= 0 {
		errGrp = append(errGrp, errors.New("GCInterval must be greater than 0"))
	}
	return errors.Join(errGrp...)
}

// New creates a new Reaper.
func New(cfg *Config) (*Reaper, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// create a cancel context to ensure all garbage collection process are shut down gracefully
	ctx, cancel := context.WithCancel(context.Background())

	return &Reaper{
		collector:    make(chan ReapParams, 10000),
		storage:      cfg.Storage,
		reapInterval: time.Duration(cfg.GCInterval) * time.Second,
		mutex:        sync.Mutex{},
		procCtx:      ctx,
		cancel:       cancel,
	}, nil
}

func (r *Reaper) Start() error {
	// Start the reaper
	go func() {
		ticker := time.NewTicker(r.reapInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.procCtx.Done():
				return
			case p := <-r.collector:
				r.write(p)
			case <-ticker.C:
				// Run the garbage collector
				r.garbageCollector()
			}
		}
	}()
	return nil
}

func (r *Reaper) Stop() error {
	// Stop the reaper collection
	close(r.collector)

	// kill the process context
	if r.cancel != nil {
		r.cancel()
	}

	// Wait for the reaper to finish
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return nil
}

func (r *Reaper) Name() string {
	return "Reaper"
}

func (r *Reaper) garbageCollector() {
	r.mutex.Lock()
	fmt.Println("Running garbage collector...")
	r.mutex.Unlock()
}
