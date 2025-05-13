package main

import (
	"context"
	"github.com/litetable/litetable-db/internal/app"
	v1 "github.com/litetable/litetable-db/internal/cdc_emitter/v1"
	"github.com/litetable/litetable-db/internal/config"
	"github.com/litetable/litetable-db/internal/operations"
	"github.com/litetable/litetable-db/internal/server"
	"github.com/litetable/litetable-db/internal/server/grpc"
	"github.com/litetable/litetable-db/internal/shard_storage"

	"github.com/litetable/litetable-db/internal/shard_storage/wal"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultDir        = ".litetable"
	defaultServerCert = "server.crt"
	defaultServerKey  = "server.key"

	// googleSeverityKey is the key used for severity in Google Cloud Logging to conform to their
	// Stackdriver logging format
	googleSeverityKey = "severity"
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

	cfg, err := config.NewConfig()
	if err != nil {
		return nil, err
	}

	initLogging(cfg)

	// load the defaults from the os.HomeDir
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	// get the filepath
	certDir := filepath.Join(homeDir, defaultDir)

	// create a new CDC Stream Server
	cdcStreamServer := v1.New()
	deps = append(deps, cdcStreamServer)

	// create the WAL manager
	walManager, err := wal.New(&wal.Config{
		Path: certDir,
	})
	if err != nil {
		return nil, err
	}

	// create a shard manager
	shardManager, garbageCollector, err := shard_storage.New(&shard_storage.Config{
		RootDir:          certDir,
		FlushThreshold:   cfg.BackupTimer,
		SnapshotTimer:    cfg.SnapshotTimer,
		MaxSnapshotLimit: cfg.MaxSnapshotLimit,
		ShardCount:       8,
		CDCEmitter:       cdcStreamServer,
	})
	if err != nil {
		return nil, err
	}

	deps = append(deps, shardManager, garbageCollector)

	opsManager, err := operations.New(&operations.Config{
		WAL:          walManager,
		ShardStorage: shardManager,
		// CDC: cdcStreamServer,
	})
	if err != nil {
		return nil, err
	}

	// create the gRPC server
	cfg.GRPCServer.Operations = opsManager
	grpcServer, err := grpc.NewServer(&cfg.GRPCServer)
	if err != nil {
		return nil, err
	}
	deps = append(deps, grpcServer)

	httpSrv, err := server.New(&cfg.Server)
	if err != nil {
		return nil, err
	}

	deps = append(deps, httpSrv)
	application, err := app.CreateApp(&app.Config{
		ServiceName: "LiteTable DB",
		StopTimeout: 30,
	}, deps...)
	if err != nil {
		return nil, err
	}

	return application, nil
}

func initLogging(cfg *config.Config) {
	// if deployed to google, change the severity key
	if cfg.CloudEnvironment == "google" {
		zerolog.LevelFieldName = googleSeverityKey
	}

	// for sanity's sake - make the dev logs easier to read and parse
	if cfg.Debug {
		output := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
			NoColor:    false,
		}
		output.FormatLevel = func(i interface{}) string {
			level, ok := i.(string)
			if !ok {
				return "???"
			}

			switch level {
			case "debug":
				return "\x1b[35m" + "DEBUG" + "\x1b[0m" // Purple for debug
			case "info":
				return "\x1b[32m" + "INFO " + "\x1b[0m" // Green for info
			case "warn":
				return "\x1b[33m" + "WARN " + "\x1b[0m" // Yellow for warn
			case "error":
				return "\x1b[31m" + "ERROR" + "\x1b[0m" // Red for error
			case "fatal", "panic":
				return "\x1b[41m" + level + "\x1b[0m" // White on red background
			default:
				return level
			}
		}

		output.Out = os.Stderr
		output.NoColor = false
		zerolog.SetGlobalLevel(zerolog.DebugLevel) // always start with debug for base logging
		// Set the global logger output
		log.Logger = zerolog.New(output).With().Timestamp().Logger()
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel) // set to info for production
	}

}
