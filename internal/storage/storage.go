package storage

// MarkRowChanged will save the row key and family name to the changedRows map.
//
// We are using an empty struct{} because it takes 0 bytes.
func (m *Manager) MarkRowChanged(family, rowKey string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.changedRows == nil {
		m.changedRows = make(map[string]map[string]struct{})
	}

	if _, exists := m.changedRows[rowKey]; !exists {
		m.changedRows[rowKey] = make(map[string]struct{})
	}

	// Add the family to the row key (this may be overwriting, but that's okay because
	// we just want to make sure the family is in the map)
	m.changedRows[rowKey][family] = struct{}{}
}
