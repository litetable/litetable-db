package ownership

/*
Ownership applies an ownership concept to litetable rows,
ensuring that only the owner of a row can access, modify or delete it.

Owners are defined by a unique identifier created by an application or service.
*/

type Manager struct {
}

func NewManager() (*Manager, error) {
	m := &Manager{}
	return m, nil
}
