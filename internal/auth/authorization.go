package auth

// Manager handles system authentication and authorization.
type Manager struct{}

// NewManager creates a new auth manager.
func NewManager() (*Manager, error) {
	return &Manager{}, nil
}
