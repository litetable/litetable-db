package litetable

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecode(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected Operation
	}{
		// Valid commands
		{
			name:     "Valid READ command",
			input:    []byte("READ database:key"),
			expected: OperationRead,
		},
		{
			name:     "Valid WRITE command",
			input:    []byte("WRITE database:key value"),
			expected: OperationWrite,
		},
		{
			name:     "Valid DELETE command",
			input:    []byte("DELETE database:key"),
			expected: OperationDelete,
		},
		{
			name:     "Valid CREATE command",
			input:    []byte("CREATE database:key"),
			expected: OperationCreate,
		},

		// Invalid commands
		{
			name:     "Empty command",
			input:    []byte(""),
			expected: OperationUnknown,
		},
		{
			name:     "Too short command",
			input:    []byte("REA"),
			expected: OperationUnknown,
		},
		{
			name:     "Missing space after READ",
			input:    []byte("READdatabase:key"),
			expected: OperationUnknown,
		},
		{
			name:     "Missing space after WRITE",
			input:    []byte("WRITEdatabase:key"),
			expected: OperationUnknown,
		},
		{
			name:     "Missing space after DELETE",
			input:    []byte("DELETEdatabase:key"),
			expected: OperationUnknown,
		},
		{
			name:     "Invalid command prefix",
			input:    []byte("INVALID command"),
			expected: OperationUnknown,
		},
		{
			name:     "Case sensitivity - read",
			input:    []byte("read database:key"),
			expected: OperationUnknown,
		},
		{
			name:     "Valid READ with trailing whitespace",
			input:    []byte(" READ database:key "),
			expected: OperationUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := Decode(tt.input)

			require.Equalf(t, tt.expected, got, "Expected operation type does not match")
		})
	}
}
