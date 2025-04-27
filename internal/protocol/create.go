package protocol

import (
	"fmt"
	"strings"
)

type CreateParams struct {
	Query              []byte
	ConfiguredFamilies []string
	CB                 func([]string) error
}

func (m *Manager) Create(params *CreateParams) error {
	input := string(params.Query)
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

	// create a copy of configured families
	newFamilies := make([]string, len(params.ConfiguredFamilies))
	copy(newFamilies, params.ConfiguredFamilies)
	
	// Add each family to the slice if it doesn't already exist
	for _, family := range families {
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

	// Call any provided callback function
	return params.CB(newFamilies)
}

// func (e *Engine) saveAllowedFamilies() error {
// 	data, err := json.Marshal(e.allowedFamilies)
// 	if err != nil {
// 		return fmt.Errorf("failed to marshal allowed families: %w", err)
// 	}
//
// 	return os.WriteFile(e.familiesFile, data, 0644)
// }
//
// func (e *Engine) loadAllowedFamilies() error {
// 	data, err := os.ReadFile(e.familiesFile)
// 	if err != nil {
// 		if os.IsNotExist(err) {
// 			// File doesn't exist yet, not an error
// 			return nil
// 		}
// 		return fmt.Errorf("failed to read allowed families file: %w", err)
// 	}
//
// 	return json.Unmarshal(data, &e.allowedFamilies)
// }
