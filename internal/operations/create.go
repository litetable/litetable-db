package operations

import (
	"strings"
)

func (m *Manager) CreateFamilies(families []string) error {
	if len(families) == 0 {
		return newError(errInvalidFormat, "creating a family requires at least one family name")
	}

	// make sure the families are not allowed currently if they are it exists
	for _, family := range families {
		if m.shardStorage.IsFamilyAllowed(family) {
			return newError(errInvalidFormat, "family %s already exists", family)
		}
	}

	// Update the shard storage with the new families
	err := m.shardStorage.UpdateFamilies(families)
	if err != nil {
		return newError(err, "failed to update families")
	}
	return nil
}

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

	err := m.shardStorage.UpdateFamilies(families)
	if err != nil {
		return newError(err, "failed to update families")
	}

	return nil
}
