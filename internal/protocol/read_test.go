package protocol

import (
	"errors"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

func TestParseRead(t *testing.T) {
	tests := map[string]struct {
		input       []byte
		expected    *readQuery
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
			expected: &readQuery{RowKey: "user:12345", Family: "main",
				Qualifiers: []string{"firstName"},
				Latest:     5, Timestamp: time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := require.New(t)
			result, err := parseRead(string(tc.input))

			if tc.expectedErr != nil {
				t.Logf("received error: %v", err.Error())
				req.True(errors.Is(err, tc.expectedErr),
					"expected error %v to wrap %v", err, tc.expectedErr)
				return
			}

			req.NotNil(result)
			req.Equal(tc.expected.RowKey, result.RowKey)
			req.Equal(tc.expected.Family, result.Family)
			req.Equal(tc.expected.Qualifiers, result.Qualifiers)
			req.Equal(tc.expected.Latest, result.Latest)
			req.Equal(tc.expected.Timestamp, result.Timestamp)
			req.Equal(tc.expected.RowKeyPrefix, result.RowKeyPrefix)
			req.Equal(tc.expected.RowKeyRegex, result.RowKeyRegex)
			req.Equal(tc.expectedErr, err)
		})
	}
}

func TestReadQuery_getLatestN(t *testing.T) {
	// test data
	values := []litetable.TimestampedValue{
		{Value: []byte("value1"), Timestamp: time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)},
		{Value: []byte("value2"), Timestamp: time.Date(2023, 10, 1, 12, 0, 1, 0, time.UTC)},
		{Value: []byte("value3"), Timestamp: time.Date(2023, 10, 1, 12, 0, 2, 0, time.UTC)},
		{Value: []byte("value4"), Timestamp: time.Date(2023, 10, 1, 12, 0, 3, 0, time.UTC)},
		{Value: []byte("value5"), Timestamp: time.Date(2023, 10, 1, 12, 0, 4, 0, time.UTC)},
		{Value: []byte("value6"), Timestamp: time.Date(2023, 10, 1, 12, 0, 5, 0, time.UTC)},
		{Value: []byte("value7"), Timestamp: time.Date(2023, 10, 1, 12, 0, 6, 0, time.UTC)},
		{Value: []byte("value8"), Timestamp: time.Date(2023, 10, 1, 12, 0, 7, 0, time.UTC)},
		{Value: []byte("value9"), Timestamp: time.Date(2023, 10, 1, 12, 0, 8, 0, time.UTC)},
		{Value: []byte("value10"), Timestamp: time.Date(2023, 10, 1, 12, 0, 9, 0, time.UTC)},
	}

	sortedValues := make([]litetable.TimestampedValue, len(values))
	copy(sortedValues, values)
	sort.Slice(sortedValues, func(i, j int) bool {
		return sortedValues[i].Timestamp.After(sortedValues[j].Timestamp)
	})

	tests := map[string]struct {
		n      int
		want   int
		values []litetable.TimestampedValue
	}{
		"get all": {
			n:      0,
			want:   len(values),
			values: sortedValues,
		},
		"get 1": {
			n:    1,
			want: 1,
			values: []litetable.TimestampedValue{
				{Value: []byte("value10"), Timestamp: time.Date(2023, 10, 1, 12, 0, 9, 0, time.UTC)},
			},
		},
		"get 2": {
			n:    2,
			want: 2,
			values: []litetable.TimestampedValue{
				{Value: []byte("value10"), Timestamp: time.Date(2023, 10, 1, 12, 0, 9, 0, time.UTC)},
				{Value: []byte("value9"), Timestamp: time.Date(2023, 10, 1, 12, 0, 8, 0, time.UTC)},
			},
		},
		"get 6": {
			n:    6,
			want: 6,
			values: []litetable.TimestampedValue{
				{Value: []byte("value10"), Timestamp: time.Date(2023, 10, 1, 12, 0, 9, 0, time.UTC)},
				{Value: []byte("value9"), Timestamp: time.Date(2023, 10, 1, 12, 0, 8, 0, time.UTC)},
				{Value: []byte("value8"), Timestamp: time.Date(2023, 10, 1, 12, 0, 7, 0, time.UTC)},
				{Value: []byte("value7"), Timestamp: time.Date(2023, 10, 1, 12, 0, 6, 0, time.UTC)},
				{Value: []byte("value6"), Timestamp: time.Date(2023, 10, 1, 12, 0, 5, 0, time.UTC)},
				{Value: []byte("value5"), Timestamp: time.Date(2023, 10, 1, 12, 0, 4, 0, time.UTC)},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := &readQuery{}
			got := r.getLatestN(values, tc.n)

			req := require.New(t)
			req.Equal(tc.want, len(got))

			// Let's check the index of the slice of tc.values to ensure we have the
			// right values
			for i, v := range got {
				req.Equal(tc.values[i].Value, v.Value)
				req.Equal(tc.values[i].Timestamp, v.Timestamp)
			}

			// lets make sure if there is more than 1 value that the times are sorted in newest
			// to oldest
			if len(got) > 1 {
				for i := 0; i < len(got)-1; i++ {
					req.True(got[i].Timestamp.After(got[i+1].Timestamp),
						"values are not sorted in descending order")
				}
			}
		})
	}
}
