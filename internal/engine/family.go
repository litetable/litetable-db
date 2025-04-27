package engine

import (
	"encoding/json"
	"fmt"
	"os"
)

func (e *Engine) saveAllowedFamilies(families []string) error {
	e.allowedFamilies = families
	data, err := json.Marshal(families)
	if err != nil {
		return fmt.Errorf("failed to marshal allowed families: %w", err)
	}
	return os.WriteFile(e.familiesFile, data, 0644)
}

func (e *Engine) loadAllowedFamilies() error {
	data, err := os.ReadFile(e.familiesFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, not an error
			return nil
		}
		return fmt.Errorf("failed to read allowed families file: %w", err)
	}

	return json.Unmarshal(data, &e.allowedFamilies)
}
