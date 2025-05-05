package config

import (
	"bufio"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	configFileName = "litetable.conf"
)

type Config struct {
	ServerAddress string
	ServerPort    string

	GarbageCollectionTimer int
	BackupTimer            int
	SnapshotTimer          int
	MaxSnapshotLimit       int
	Debug                  bool
	CloudEnvironment       string
}

func NewConfig() (*Config, error) {
	liteTableDir, err := litetable.GetLitetableDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get LiteTable directory: %w", err)
	}

	configPath := filepath.Join(liteTableDir, configFileName)
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("LiteTable is not installed or configuration file not found")
	}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	config := &Config{}
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "server_address":
			config.ServerAddress = value
		case "server_port":
			config.ServerPort = value
		case "backup_timer":
			config.BackupTimer, err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid backup timer value: %w", err)
			}
		case "garbage_collection_timer":
			config.GarbageCollectionTimer, err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid garbage collection timer value: %w", err)
			}
		case "debug":
			config.Debug = value == "true"
		case "cloud_environment":
			config.CloudEnvironment = value
		case "snapshot_timer":
			config.SnapshotTimer, err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid snapshot timer value: %w", err)
			}
		case "max_snapshot_limit":
			config.MaxSnapshotLimit, err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("invalid snapshot limit value: %w", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	return config, nil
}
