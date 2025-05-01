package operations

import (
	"fmt"
	"strings"
)

func (m *Manager) create(query []byte) error {
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

	// Call any provided callback function
	return m.storage.UpdateFamilies(families)
}
