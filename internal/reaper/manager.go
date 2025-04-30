package reaper

import (
	"context"
	"errors"
	"github.com/litetable/litetable-db/internal/litetable"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	reaperFile = ".reaper.gc.log"
)

type storage interface {
	GetData() *litetable.Data
	RWLock() // The Reaper needs control to lock and unlock the data source
	RWUnlock()
}

type Reaper struct {
	filePath  string
	collector chan GCParams
	storage   storage

	mutex        sync.Mutex
	reapInterval time.Duration

	procCtx context.Context
	cancel  context.CancelFunc
}

type Config struct {
	Path       string
	Storage    storage
	GCInterval int
}

func (c *Config) validate() error {
	var errGrp []error
	if c.Path == "" {
		errGrp = append(errGrp, errors.New("directory path cannot be empty"))
	}
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

	filePath := filepath.Join(cfg.Path, reaperFile)
	// create a cancel context to ensure all garbage collection processes are shut down gracefully
	ctx, cancel := context.WithCancel(context.Background())

	return &Reaper{
		filePath:     filePath,
		collector:    make(chan GCParams, 10000),
		storage:      cfg.Storage,
		reapInterval: time.Duration(cfg.GCInterval) * time.Second,
		mutex:        sync.Mutex{},
		procCtx:      ctx,
		cancel:       cancel,
	}, nil
}

func (r *Reaper) Start() error {
	// Verify the log file exists
	if err := r.verifyLogFile(); err != nil {
		return err
	}

	// Start the reaper
	go func() {
		ticker := time.NewTicker(r.reapInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.procCtx.Done():
				return
			case p := <-r.collector:
				r.write(&p)
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

// verifyLogFile checks if the log file exists, and creates it if it doesn't.
func (r *Reaper) verifyLogFile() error {
	_, err := os.Stat(r.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err // Return error if it's something other than "file not exist"
		}

		// Create file if it doesn't exist
		file, fileErr := os.Create(r.filePath)
		if fileErr != nil {
			return fileErr
		}
		defer func(file *os.File) {
			_ = file.Close()
		}(file)
		return nil // Successfully created the file
	}
	return nil // File already exists
}
