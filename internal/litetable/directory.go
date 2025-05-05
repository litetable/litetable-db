package litetable

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	litetableDir = ".litetable"
)

// GetLitetableDir returns the path to the LiteTable directory in the user's home directory.
func GetLitetableDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	certDir := filepath.Join(homeDir, litetableDir)

	return certDir, nil
}
