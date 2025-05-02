package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

//go:generate mockgen -destination=./app_mock.go -package=app -source=app.go

// Dependency is the interface that wraps the basic methods of a dependency required for the application.
type Dependency interface {
	// Start is anything a dependency needs to do before it's ready to be used
	Start() error
	// Stop is anything a dependency needs to do before it's ready to be stopped
	Stop() error
	// Name is the name of the dependency. It is used for logging and identification purposes, only.
	Name() string
}

type App struct {
	serviceName string
	// Deps is a list of dependencies that the application will start.
	deps []Dependency
	// depFailChan is a channel that will be used to signal when a dependency has failed to start.
	depFailChan chan error
	// osSignalChan is a channel that will be used to signal when the OS has sent a signal to the application.
	osSignalChan chan os.Signal
	// stopCalled is an atomic bool. It allows stop to be called once
	stopCalled *atomic.Bool
	// runCalled allows Start to be called once
	runCalled *atomic.Bool
	// stopTimeout is the amount of time the application will wait for dependencies to stop before exiting.
	stopTimeout time.Duration
}

type Config struct {
	ServiceName string
	StopTimeout time.Duration
}

func (c *Config) validate() error {
	var errs []error
	if c.ServiceName == "" {
		errs = append(errs, errors.New("service name is required"))
	}
	if c.StopTimeout == 0 {
		errs = append(errs, errors.New("stop timeout is required"))
	}
	return errors.Join(errs...)
}

// CreateApp creates a new application with the provided dependencies.
func CreateApp(cfg *Config, deps ...Dependency) (*App, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &App{
		serviceName:  cfg.ServiceName,
		deps:         deps,
		stopTimeout:  cfg.StopTimeout,
		stopCalled:   &atomic.Bool{},
		runCalled:    &atomic.Bool{},
		depFailChan:  make(chan error, len(deps)), // only 1 channel for each dependency
		osSignalChan: make(chan os.Signal, 1),     // first signal we get shuts down the app
	}, nil
}

// Run starts all dependencies in the application.
func (a *App) Run(ctx context.Context) error {
	// This first call is defensive because Run is a public function. We do not want a consumer
	// to call this more than once.
	if a.runCalled.Load() {
		return errors.New("run has already been called")
	}

	// defer funcs are always LIFO - don't forget!
	ctxCancel, cancel := context.WithCancel(ctx) // we are cancelling the consumers context
	defer func() {
		close(a.depFailChan)
		close(a.osSignalChan)
		cancel() // cancel would be called first, then close the channels
	}()

	// TODO: probably want to handle a panic

	// Start all dependencies
	for _, dep := range a.deps {
		// Each dependency exists in its own goroutine. Some deps like a grpc server will run
		// inside the goroutine until the SIGTERM is received. We should never block, but listen for
		// failures
		go func(dep Dependency) {
			defer func() {
				if err := recover(); err != nil {
					// if error, throw error into depFailChan
					a.depFailChan <- fmt.Errorf("panic in Start() for dependency %s: %v", dep.Name(), err)
				}
			}()

			log.Info().Msg("Starting dependency: " + dep.Name())
			err := dep.Start()
			if err != nil {
				a.depFailChan <- fmt.Errorf("failure in Start() for dependency %s: %v", dep.Name(),
					err)
			}
		}(dep)
	}

	// here we are waiting for a signal from the OS or a failure from a dependency,
	// or the ctx to just cancel
	signal.Notify(a.osSignalChan, os.Interrupt, syscall.SIGTERM)
	select {
	case <-ctxCancel.Done():
		log.Info().Msg("App Context cancelled: shutting down")
	case depErr := <-a.depFailChan:
		log.Error().Msg("Dependency failed to start: " + depErr.Error())
	case sig := <-a.osSignalChan:
		log.Info().Msg("OS Signal received: " + sig.String() + " shutdown beginning...")
	}

	// Stop all dependencies
	signal.Stop(a.osSignalChan)
	if err := a.stop(); err != nil {
		log.Error().Msg("Error stopping application: " + err.Error())
		return err
	}

	return nil
}

// stop attempts a graceful shutdown of each dependency.
func (a *App) stop() error {
	if a.stopCalled.Load() {
		return errors.New("stop has already been called")
	}

	// set stopCalled to true
	a.stopCalled.Store(true)

	ctxTo, cancel := context.WithTimeout(context.Background(), a.stopTimeout+60*time.Second)

	var errs []error

	go func(ctx context.Context) {
		defer cancel()

		for _, dep := range a.deps {
			log.Info().Msg("Stopping dependency: " + dep.Name())
			if err := dep.Stop(); err != nil {
				errs = append(errs, fmt.Errorf("failure in Stop() for dependency %s: %v", dep.Name(), err))
			}
		}
	}(ctxTo)

	// we need all dependencies to stop before we can return: block until ctxTo is done
	<-ctxTo.Done()

	// check the ctxTo for errors. This could have been a timeout
	if err := ctxTo.Err(); errors.Is(err, context.DeadlineExceeded) {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
