package system

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// DefaultDir returns the default system directory path based on the operating system.
// This path is not user-configurable and is used for storing system-related data.
func DefaultDir() string {
	switch runtime.GOOS {
	case "linux":
		return "/var/lib/litetable/system"
	case "darwin":
		return "/usr/local/var/litetable/system"
	case "windows":
		return `C:\ProgramData\LiteTable\system`
	default:
		// Fallback for unknown OSes. Still not user-configurable.
		home, _ := os.UserHomeDir()
		return filepath.Join(home, ".litetable", "system")
	}
}

func EnsureSecureSystemDirectory(dir string) (string, error) {
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
