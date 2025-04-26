package protocol

import (
	"testing"
)

func TestDecode(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected int
		wantErr  bool
	}{
		// Valid commands
		{
			name:     "Valid READ command",
			input:    []byte("READ database:key"),
			expected: Read,
			wantErr:  false,
		},
		{
			name:     "Valid WRITE command",
			input:    []byte("WRITE database:key value"),
			expected: Write,
			wantErr:  false,
		},
		{
			name:     "Valid DELETE command",
			input:    []byte("DELETE database:key"),
			expected: Delete,
			wantErr:  false,
		},

		// Invalid commands
		{
			name:     "Empty command",
			input:    []byte(""),
			expected: Unknown,
			wantErr:  true,
		},
		{
			name:     "Too short command",
			input:    []byte("REA"),
			expected: Unknown,
			wantErr:  true,
		},
		{
			name:     "Missing space after READ",
			input:    []byte("READdatabase:key"),
			expected: Unknown,
			wantErr:  true,
		},
		{
			name:     "Missing space after WRITE",
			input:    []byte("WRITEdatabase:key"),
			expected: Unknown,
			wantErr:  true,
		},
		{
			name:     "Missing space after DELETE",
			input:    []byte("DELETEdatabase:key"),
			expected: Unknown,
			wantErr:  true,
		},
		{
			name:     "Invalid command prefix",
			input:    []byte("INVALID command"),
			expected: Unknown,
			wantErr:  true,
		},
		{
			name:     "Case sensitivity - read",
			input:    []byte("read database:key"),
			expected: Unknown,
			wantErr:  true,
		},
		{
			name:     "Valid READ with trailing whitespace",
			input:    []byte("READ database:key "),
			expected: Read,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := Decode(tt.input)

			// Check error expectation
			if (err != nil) != tt.wantErr {
				t.Errorf("Decode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// If no error is expected, verify the result
			if !tt.wantErr && got != tt.expected {
				t.Errorf("Decode() = %v, want %v", got, tt.expected)
			}
		})
	}
}
