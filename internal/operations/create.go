package operations

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
