package protocol

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParseRead(t *testing.T) {
	tests := map[string]struct {
		input       []byte
		expected    *ReadQuery
		expectedErr error
	}{
		"unknown parameter": {
			input:       []byte("pizza=pepperoni"),
			expectedErr: ErrUnknownParameter,
		},
		"invalid format": {
			input:       []byte("family key=value"),
			expectedErr: ErrInvalidFormat,
		},
		"invalid latest value: text number": {
			input:       []byte("family=col1 latest=one"),
			expectedErr: ErrInvalidFormat,
		},
		"invalid latest value: negative number": {
			input:       []byte("family=col1 latest=-1"),
			expectedErr: ErrInvalidFormat,
		},
		"invalid timestamp format": {
			input:       []byte("family=col1 timestamp=2023-10-01"),
			expectedErr: ErrInvalidFormat,
		},
		"missing search key - key": {
			input:       []byte("qualifier=col1"),
			expectedErr: ErrMissingKey,
		},
		"too many search keys - key and prefix": {
			input:       []byte("key=key1 prefix=key2 family=col1"),
			expectedErr: ErrInvalidFormat,
		},
		"too many search keys - key and regex": {
			input:       []byte("key=key1 regex=key2 family=col1"),
			expectedErr: ErrInvalidFormat,
		},
		"too many search keys - prefix and regex": {
			input:       []byte("prefix=key1 regex=key2 family=col1"),
			expectedErr: ErrInvalidFormat,
		},
		"missing family": {
			input:       []byte("key=key1"),
			expectedErr: ErrInvalidFormat,
		},
		"valid read query": {
			input: []byte("key=user:12345 family=main qualifier=firstName latest=5 timestamp=2023" +
				"-10" +
				"-01T12:00:00Z"),
			expected: &ReadQuery{rowKey: "user:12345", family: "main",
				qualifiers: []string{"firstName"},
				latest:     5, timestamp: time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := require.New(t)
			result, err := ParseRead(string(tc.input))

			if tc.expectedErr != nil {
				t.Logf("received error: %v", err.Error())
				req.True(errors.Is(err, tc.expectedErr),
					"expected error %v to wrap %v", err, tc.expectedErr)
				return
			}

			req.NotNil(result)
			req.Equal(tc.expected.rowKey, result.rowKey)
			req.Equal(tc.expected.family, result.family)
			req.Equal(tc.expected.qualifiers, result.qualifiers)
			req.Equal(tc.expected.latest, result.latest)
			req.Equal(tc.expected.timestamp, result.timestamp)
			req.Equal(tc.expected.rowKeyPrefix, result.rowKeyPrefix)
			req.Equal(tc.expected.rowKeyRegex, result.rowKeyRegex)
			req.Equal(tc.expectedErr, err)
		})
	}
}
