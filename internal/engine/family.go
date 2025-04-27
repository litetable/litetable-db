package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func (e *Engine) CreateFamily(query []byte) error {
	parsed, err := parseCreateFamilyQuery(string(query))
	if err != nil {
		return err
	}

	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	// Check if family already exists
	for _, family := range e.allowedFamilies {
		if family == parsed.family {
			// Family already exists, no need to add it again
			return nil
		}
	}

	// Add the family to allowed families
	e.allowedFamilies = append(e.allowedFamilies, parsed.family)

	// Persist to disk
	return e.saveAllowedFamilies()
}

type createFamilyQuery struct {
	family  string
	columns []string
}

func parseCreateFamilyQuery(input string) (*createFamilyQuery, error) {
	parts := strings.Fields(input)
	parsed := &createFamilyQuery{}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid format: %s", part)
		}

		key, value := kv[0], kv[1]
		if key == "family" {
			parsed.family = value
		}
	}

	if parsed.family == "" {
		return nil, fmt.Errorf("missing family name")
	}

	return parsed, nil
}

func (e *Engine) createFamily(query []byte) error {
	input := string(query)
	parts := strings.Fields(input)

	var familyValue string
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid format: %s", part)
		}

		key, value := kv[0], kv[1]
		if key == "family" {
			familyValue = value
			break
		}
	}

	if familyValue == "" {
		return fmt.Errorf("missing family name")
	}

	// Split the family value by commas to get individual family names
	families := strings.Split(familyValue, ",")

	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	// Add each family to the slice if it doesn't already exist
	for _, family := range families {
		family = strings.TrimSpace(family)
		if family == "" {
			continue
		}

		exists := false
		for _, existing := range e.allowedFamilies {
			if existing == family {
				exists = true
				break
			}
		}

		if !exists {
			e.allowedFamilies = append(e.allowedFamilies, family)
		}
	}

	// Persist allowed families to disk
	return e.saveAllowedFamilies()
}

func (e *Engine) isFamilyAllowed(family string) bool {
	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()

	// If no allowed families are defined, don't allow any
	if len(e.allowedFamilies) == 0 {
		return false
	}

	for _, f := range e.allowedFamilies {
		if f == family {
			return true
		}
	}
	return false
}

func (e *Engine) saveAllowedFamilies() error {
	data, err := json.Marshal(e.allowedFamilies)
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
