package main

import (
	"context"
	"crypto/tls"
	"github.com/litetable/litetable-db/internal/app"
	"github.com/litetable/litetable-db/internal/engine"
	"github.com/litetable/litetable-db/internal/protocol"
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

	protocolManager := protocol.New()

	// create a disk storage manager
	diskStorage, err := storage.NewDiskStorage(&storage.Config{
		RootDir:        certDir,
		FlushThreshold: 1000,
	})
	if err != nil {
		return nil, err
	}

	// create the WAL manager
	walManager, err := wal.New(&wal.Config{})
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
