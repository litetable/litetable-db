package operations

import (
	"strings"
)

func (m *Manager) create(query []byte) error {
	input := string(query)
	parts := strings.Fields(input)

	var familyValue string
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return newError(errInvalidFormat, "%s", part)
		}

		key, value := kv[0], kv[1]
		if key == "family" {
			familyValue = value
			break
		}
	}

	if familyValue == "" {
		return newError(errMissingKey, "missing family name")
	}

	// Split the family value by commas to get individual family names
	families := strings.Split(familyValue, ",")

	err := m.storage.UpdateFamilies(families)
	if err != nil {
		return newError(err, "failed to update families")
	}

	return nil
}
