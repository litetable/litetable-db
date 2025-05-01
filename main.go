package main

import (
	"context"
	"crypto/tls"
	"github.com/litetable/litetable-db/internal/app"
	"github.com/litetable/litetable-db/internal/engine"
	"github.com/litetable/litetable-db/internal/protocol"
	"github.com/litetable/litetable-db/internal/reaper"
	"github.com/litetable/litetable-db/internal/server"
	"github.com/litetable/litetable-db/internal/storage"
	"github.com/litetable/litetable-db/internal/wal"
	"os"
	"path/filepath"
)

const (
	defaultDir        = ".litetable"
	defaultServerCert = "server.crt"
	defaultServerKey  = "server.key"
)

func main() {
	application, err := initialize()
	if err != nil {
		panic(err)
	}

	if err = application.Run(context.Background()); err != nil {
		panic(err)
	}
}

func initialize() (*app.App, error) {
	var deps []app.Dependency

	// load the defaults from the os.HomeDir
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	// get the filepath
	certDir := filepath.Join(homeDir, defaultDir)

	// load the TLS certificate and key, ideally this is configuration based on deployments, but
	// for now we can roll with it.
	// TODO: make certificate requirements configurable
	cert, err := tls.LoadX509KeyPair(certDir+"/"+defaultServerCert, certDir+"/"+defaultServerKey)
	if err != nil {
		return nil, err
	}

	// create a disk storage manager
	diskStorage, err := storage.New(&storage.Config{
		RootDir:          certDir,
		FlushThreshold:   20,
		MaxSnapshotLimit: 2,
	})
	if err != nil {
		return nil, err
	}

	deps = append(deps, diskStorage)

	// create a new Reaper (aka Garbage Collector)
	reaperGC, err := reaper.New(&reaper.Config{
		Storage:    diskStorage,
		GCInterval: 30,
		Path:       certDir,
	})
	if err != nil {
		return nil, err
	}

	deps = append(deps, reaperGC)

	// create the WAL manager
	walManager, err := wal.New(&wal.Config{
		Path: certDir,
	})
	if err != nil {
		return nil, err
	}

	// Protocol is the package that interacts with the LiteTable Data. It decides how to read and write
	// data to the disk storage and sends values for Garbage Collection.
	protocolManager, err := protocol.New(&protocol.Config{
		GarbageCollector: reaperGC,
		WAL:              walManager,
		Storage:          diskStorage,
	})
	if err != nil {
		return nil, err
	}

	// create the litetable engine
	engineHandler, err := engine.New(&engine.Config{
		WAL:      walManager,
		Storage:  diskStorage,
		Protocol: protocolManager,
	})
	if err != nil {
		return nil, err
	}

	deps = append(deps, engineHandler)

	// create a LiteTable server
	srv, err := server.New(&server.Config{
		Certificate: &cert,
		Port:        "9443",
		Handler:     engineHandler,
	})
	if err != nil {
		return nil, err
	}

	deps = append(deps, srv)

	application, err := app.CreateApp(&app.Config{
		ServiceName: "LiteTable DB",
		StopTimeout: 5,
	}, deps...)
	if err != nil {
		return nil, err
	}

	return application, nil
}
