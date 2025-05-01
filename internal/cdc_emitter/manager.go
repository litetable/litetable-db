package cdc_emitter

type Manager struct{}

func New() (*Manager, error) {
	return &Manager{}, nil
}

func (m *Manager) Start() error {
	return nil
}

func (m *Manager) Stop() error {
	return nil
}

func (m *Manager) Name() string {
	return "CDC Emitter"
}
