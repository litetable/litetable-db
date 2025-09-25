package system

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	DefaultLitetableDir = ".litetable"
)

// GetLitetableDir returns the path to the LiteTable directory in the user's home directory.
func GetLitetableDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	certDir := filepath.Join(homeDir, DefaultLitetableDir)

	return certDir, nil
}

// DefaultDir returns the default system directory path based on the operating system.
// This path is not user-configurable and is used for storing system-related data.
func DefaultDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		// fallback to current working dir if somehow home is unavailable
		return filepath.Join(".", ".litetable", "system")
	}
	return filepath.Join(home, ".litetable", "system")
}

func EnsureSecureSystemDirectory() (string, error) {
	dir := DefaultDir()

	// Create if missing
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create system dir: %w", err)
	}
	// Enforce perms on POSIX (Windows ignores this)
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", fmt.Errorf("chmod system dir: %w", err)
	}
	return dir, nil
}
