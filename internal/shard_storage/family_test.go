package shard_storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_isFamilyAllowed(t *testing.T) {
	tests := map[string]struct {
		allowed  []string
		family   string
		expected bool
	}{
		"no allowed families": {
			allowed: []string{},
			family:  "family1",
		},
		"allowed family": {
			allowed:  []string{"family1", "family2"},
			family:   "family1",
			expected: true,
		},
		"not allowed family": {
			allowed: []string{"family1", "family2"},
			family:  "family3",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := &Manager{
				allowedFamilies: tc.allowed,
			}
			result := m.IsFamilyAllowed(tc.family)
			assert.Equal(t, tc.expected, result)
		})
	}
}
