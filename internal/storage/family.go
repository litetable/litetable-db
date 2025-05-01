package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func (m *Manager) UpdateFamilies(new []string) error {
	// create a copy of configured families
	newFamilies := make([]string, len(m.allowedFamilies))
	copy(newFamilies, m.allowedFamilies)

	// Add each family to the slice if it doesn't already exist
	for _, family := range new {
		family = strings.TrimSpace(family)
		if family == "" {
			continue
		}

		exists := false
		for _, existing := range newFamilies {
			if existing == family {
				exists = true
				break
			}
		}

		if !exists {
			newFamilies = append(newFamilies, family)
		}
	}

	// save to the struct
	m.mutex.Lock()
	m.allowedFamilies = newFamilies
	m.mutex.Unlock()

	data, err := json.Marshal(newFamilies)
	if err != nil {
		return fmt.Errorf("failed to marshal allowed families: %w", err)
	}
	return os.WriteFile(m.familiesFile, data, 0644)
}

func (m *Manager) loadAllowedFamilies() error {
	data, err := os.ReadFile(m.familiesFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist yet, not an error
			return nil
		}
		return fmt.Errorf("failed to read allowed families file: %w", err)
	}

	return json.Unmarshal(data, &m.allowedFamilies)
}

func (m *Manager) GetFamilies() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// create a copy of the slice to avoid
	// concurrent read and write issues
	newFamilies := make([]string, len(m.allowedFamilies))
	copy(newFamilies, m.allowedFamilies)
	return newFamilies
}

func (m *Manager) IsFamilyAllowed(family string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// If no allowed families are defined, don't allow any
	if len(m.allowedFamilies) == 0 {
		return false
	}

	for _, f := range m.allowedFamilies {
		if f == family {
			return true
		}
	}
	return false
}
