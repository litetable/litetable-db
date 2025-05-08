package operations

import (
	"errors"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestParseRead(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		input       []byte
		expected    *readQuery
		expectedErr error
	}{
		"unknown parameter": {
			input:       []byte("pizza=pepperoni"),
			expectedErr: errUnknownParameter,
		},
		"invalid format": {
			input:       []byte("family key=value"),
			expectedErr: errInvalidFormat,
		},
		"invalid latest value: text number": {
			input:       []byte("family=col1 latest=one"),
			expectedErr: errInvalidFormat,
		},
		"invalid latest value: negative number": {
			input:       []byte("family=col1 latest=-1"),
			expectedErr: errInvalidFormat,
		},
		"invalid timestamp format": {
			input:       []byte("family=col1 timestamp=2023-10-01"),
			expectedErr: errInvalidFormat,
		},
		"missing search key - key": {
			input:       []byte("qualifier=col1"),
			expectedErr: errMissingKey,
		},
		"too many search keys - key and prefix": {
			input:       []byte("key=key1 prefix=key2 family=col1"),
			expectedErr: errInvalidFormat,
		},
		"too many search keys - key and regex": {
			input:       []byte("key=key1 regex=key2 family=col1"),
			expectedErr: errInvalidFormat,
		},
		"too many search keys - prefix and regex": {
			input:       []byte("prefix=key1 regex=key2 family=col1"),
			expectedErr: errInvalidFormat,
		},
		"missing family": {
			input:       []byte("key=key1"),
			expectedErr: errInvalidFormat,
		},
		"valid read query": {
			input: []byte("key=user:12345 family=main qualifier=firstName latest=5 timestamp=2023" +
				"-10" +
				"-01T12:00:00Z"),
			expected: &readQuery{rowKey: "user:12345", family: "main",
				qualifiers: []string{"firstName"},
				latest:     5, timestamp: time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)},
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

func TestReadQuery_getLatestN(t *testing.T) {
	t.Parallel()
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

func TestReadQuery_getLatestN_withTombstones(t *testing.T) {
	t.Parallel()
	// test data
	values := []litetable.TimestampedValue{
		{Value: []byte("value1"), Timestamp: time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)},
		{Value: []byte("value2"), Timestamp: time.Date(2023, 10, 1, 12, 0, 1, 0, time.UTC)},
		{Value: []byte("value3"), Timestamp: time.Date(2023, 10, 1, 12, 0, 2, 0, time.UTC)},
		{Value: []byte("value4"), Timestamp: time.Date(2023, 10, 1, 12, 0, 3, 0, time.UTC)},
		{Value: []byte("value5"), Timestamp: time.Date(2023, 10, 1, 12, 0, 4, 0, time.UTC)},
		{Value: []byte("value7"), Timestamp: time.Date(2023, 10, 1, 12, 0, 5, 0, time.UTC)},
		{Value: []byte("value7"), IsTombstone: true, Timestamp: time.Date(2023, 10, 1, 12, 0, 5, 0, time.UTC)},
		{Value: []byte("value8"), Timestamp: time.Date(2023, 10, 1, 12, 0, 7, 0, time.UTC)},
		{Value: []byte("value9"), Timestamp: time.Date(2023, 10, 1, 12, 0, 8, 0,
			time.UTC)},
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
		"tombstone": {
			n:      0,
			want:   3,
			values: sortedValues,
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

// Test_readRowKey tests the readRowKey against the parsed params.
func Test_readRowKey(t *testing.T) {
	t.Parallel()

	now := time.Now()
	mockdata := &litetable.Data{
		"user:12345": {
			"profile": {
				"firstName": {
					{Value: []byte("John"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Smith"), Timestamp: now},
					{Value: []byte("Smithy"), Timestamp: now.Add(-time.Hour)},
				},
				"email": {
					{Value: []byte("john@example.net"), Timestamp: now},
					{Value: []byte("john@example.com"), Timestamp: now.Add(-time.Hour)},
				},
			},
		},
	}

	tests := map[string]struct {
		rq       *readQuery
		mockdata *litetable.Data

		expected             *litetable.Row
		unexpectedQualifiers []string
		expectedErr          error
	}{
		"row not found": {
			rq: &readQuery{
				rowKey: "user:12345",
			},
			mockdata:    &litetable.Data{},
			expectedErr: errors.New("row not found: user:12345"),
		},
		"family not found": {
			rq: &readQuery{
				rowKey: "user:12345",
				family: "wrestlers",
			},
			mockdata: &litetable.Data{
				"user:12345": {
					"main": {},
				},
			},
			expectedErr: errors.New("family not found: wrestlers"),
		},
		"no qualifiers returns full family": {
			rq: &readQuery{
				rowKey: "user:12345",
				family: "profile",
				latest: 0,
			},
			mockdata: mockdata,
			expected: &litetable.Row{
				Key: "user:12345",
				Columns: map[string]litetable.VersionedQualifier{
					"profile": {
						"firstName": {
							{Value: []byte("John"), Timestamp: now},
						},
						"lastName": {
							{Value: []byte("Smith"), Timestamp: now},
							{Value: []byte("Smithy"), Timestamp: now.Add(-time.Hour)},
						},
						"email": {
							{Value: []byte("john@example.net"), Timestamp: now},
							{Value: []byte("john@example.com"), Timestamp: now.Add(-time.Hour)},
						},
					},
				},
			},
		},
		"no qualifiers with latestN=1 returns only latest value of each qualifier": {
			rq: &readQuery{
				rowKey: "user:12345",
				family: "profile",
				latest: 1,
			},
			mockdata: mockdata,
			expected: &litetable.Row{
				Key: "user:12345",
				Columns: map[string]litetable.VersionedQualifier{
					"profile": {
						"firstName": {
							{Value: []byte("John"), Timestamp: now},
						},
						"lastName": {
							{Value: []byte("Smith"), Timestamp: now},
						},
						"email": {
							{Value: []byte("john@example.net"), Timestamp: now},
						},
					},
				},
			},
		},
		"single qualifier returns only that column": {
			rq: &readQuery{
				rowKey:     "user:12345",
				family:     "profile",
				qualifiers: []string{"lastName"},
				latest:     0,
			},
			mockdata:             mockdata,
			unexpectedQualifiers: []string{"firstName", "email"},
			expected: &litetable.Row{
				Key: "user:12345",
				Columns: map[string]litetable.VersionedQualifier{
					"profile": {
						"lastName": {
							{Value: []byte("Smith"), Timestamp: now},
							{Value: []byte("Smithy"), Timestamp: now.Add(-time.Hour)},
						},
					},
				},
			},
		},
		"single qualifier with latest1 returns only 1 record": {
			rq: &readQuery{
				rowKey:     "user:12345",
				family:     "profile",
				qualifiers: []string{"lastName"},
				latest:     1,
			},
			mockdata:             mockdata,
			unexpectedQualifiers: []string{"firstName", "email"},
			expected: &litetable.Row{
				Key: "user:12345",
				Columns: map[string]litetable.VersionedQualifier{
					"profile": {
						"lastName": {
							{Value: []byte("Smith"), Timestamp: now},
						},
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := require.New(t)

			row, err := tc.rq.readRowKey(tc.mockdata)
			if tc.expectedErr != nil {
				req.Equal(tc.expectedErr.Error(), err.Error())
			} else {
				req.NoError(err)
				req.NotNil(row)
				req.Contains(row.Columns, tc.rq.family)
				req.Equal(tc.expected.Key, row.Key)
				mockFamily := (*tc.mockdata)[tc.rq.rowKey][tc.rq.family]

				// if the latest is > 0 the columns should equal that number
				if tc.rq.latest > 0 {
					for qual, _ := range row.Columns[tc.rq.family] {
						req.Equal(len(row.Columns[tc.rq.family][qual]), tc.rq.latest)
					}
				} else {
					// for each qualifier and value in the response,
					// ensure the same number of values
					// and that the qualifier is in the mock data
					for qualifier, values := range row.Columns[tc.rq.family] {
						// make sure the qualifier is in the mock data
						req.Contains(mockFamily, qualifier)
						// ensure the same number of values as the mockdata
						req.Equal(len(mockFamily[qualifier]), len(values))
					}
				}

				// make sure the only qualifiers returned are the ones in the test array
				if len(tc.rq.qualifiers) > 0 {
					// make sure the qualifier amounts match
					req.Equal(len(tc.rq.qualifiers), len(row.Columns[tc.rq.family]))

					// for every unexpected qualifier, make sure it is not in the response
					for _, uq := range tc.unexpectedQualifiers {
						_, exists := row.Columns[tc.rq.family][uq]
						req.False(exists, "unexpected qualifier %s found in response", uq)
					}
				}
			}
		})
	}
}

func Test_filterRowsByPrefix(t *testing.T) {
	now := time.Now()
	mockdata := &litetable.Data{
		"user:12345": {
			"profile": {
				"firstName": {
					{Value: []byte("John"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Smith"), Timestamp: now},
				},
				"email": {
					{Value: []byte("john@example.net"), Timestamp: now},
				},
			},
		},
		"user:12567": {
			"profile": {
				"firstName": {
					{Value: []byte("Ruby"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Rocket"), Timestamp: now},
				},
				"email": {
					{Value: []byte("ruby@example.net"), Timestamp: now},
				},
			},
		},
		"user:56789": {
			"profile": {
				"firstName": {
					{Value: []byte("Rosie"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Rocket"), Timestamp: now},
				},
				"email": {
					{Value: []byte("rosie@example.net"), Timestamp: now},
				},
			},
		},
	}

	tests := map[string]struct {
		rq                   *readQuery
		resultCount          int
		expectedKeys         []string
		expectedErr          error
		unexpectedQualifiers []string
	}{
		"no prefix match": {
			rq: &readQuery{
				rowKeyPrefix: "user:99999",
				family:       "profile",
			},
			resultCount:  0,
			expectedKeys: []string{},
			expectedErr:  errors.New("no rows found with prefix: user:99999"),
		},
		"prefix match only returns that row": {
			rq: &readQuery{
				rowKeyPrefix: "user:12345",
				family:       "profile",
			},
			resultCount:  1,
			expectedKeys: []string{"firstName", "lastName", "email"},
		},
		"prefix filter returns expected row count: 2": {
			rq: &readQuery{
				rowKeyPrefix: "user:12",
				family:       "profile",
			},
			resultCount:  2,
			expectedKeys: []string{"firstName", "lastName", "email"},
		},
		"prefix match with qualifier only returns that row and qualifier": {
			rq: &readQuery{
				rowKeyPrefix: "user:12345",
				family:       "profile",
				qualifiers:   []string{"firstName"},
			},
			resultCount:          1,
			expectedKeys:         []string{"firstName"},
			unexpectedQualifiers: []string{"lastName", "email"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := require.New(t)
			row, err := tc.rq.filterRowsByPrefix(mockdata)

			if tc.expectedErr != nil {
				req.Equal(tc.expectedErr.Error(), err.Error())
			} else {
				req.NoError(err)
				req.Equal(tc.resultCount, len(row))
				for _, key := range tc.expectedKeys {
					_, exists := row["user:12345"].Columns[tc.rq.family][key]
					req.True(exists, "expected key %s not found in result", key)
				}

				for _, uq := range tc.unexpectedQualifiers {
					for rowKey, rowData := range row {
						if strings.HasPrefix(rowKey, tc.rq.rowKeyPrefix) {
							_, exists := rowData.Columns[tc.rq.family][uq]
							req.False(exists, "unexpected qualifier %s found in response", uq)
						}
					}
				}
			}
		})
	}
}

func Test_filterRowsByRegex(t *testing.T) {
	now := time.Now()
	mockdata := &litetable.Data{
		"user:12345": {
			"profile": {
				"firstName": {
					{Value: []byte("John"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Smith"), Timestamp: now},
				},
				"email": {
					{Value: []byte("john@example.net"), Timestamp: now},
				},
			},
		},
		"user:12567": {
			"profile": {
				"firstName": {
					{Value: []byte("Ruby"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Rocket"), Timestamp: now},
				},
				"email": {
					{Value: []byte("ruby@example.net"), Timestamp: now},
				},
			},
		},
		"user:56789": {
			"profile": {
				"firstName": {
					{Value: []byte("Rosie"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Rocket"), Timestamp: now},
				},
				"email": {
					{Value: []byte("rosie@example.net"), Timestamp: now},
				},
			},
		},
	}

	tests := map[string]struct {
		rq                   *readQuery
		resultCount          int
		expectedKeys         []string
		expectedErr          error
		unexpectedQualifiers []string
	}{
		"regex doesn't compile": {
			rq: &readQuery{
				rowKeyRegex: "*.user.*",
				family:      "profile",
			},
			resultCount:  0,
			expectedKeys: []string{},
			expectedErr:  errors.New("invalid regex pattern: error parsing regexp: missing argument to repetition operator: `*`"),
		},
		"regex finds no matches": {
			rq: &readQuery{
				rowKeyRegex: "customer:[0-9]+",
				family:      "profile",
			},
			resultCount:  0,
			expectedKeys: []string{},
			expectedErr:  errors.New("no rows found matching regex: customer:[0-9]+"),
		},
		"regex filter returns expected row count: 2": {
			rq: &readQuery{
				rowKeyRegex: "12",
				family:      "profile",
			},
			resultCount:  2,
			expectedKeys: []string{"firstName", "lastName", "email"},
		},
		"regex match with qualifier only returns rowKeys and qualifier": {
			rq: &readQuery{
				rowKeyRegex: "678",
				family:      "profile",
				qualifiers:  []string{"firstName"},
			},
			resultCount:          1,
			expectedKeys:         []string{"firstName"},
			unexpectedQualifiers: []string{"lastName", "email"},
		},
		"regex returns all rows in wide query": {
			rq: &readQuery{
				rowKeyRegex: "user:[0-9]+",
				family:      "profile",
			},
			resultCount:  3,
			expectedKeys: []string{"firstName", "lastName", "email"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := require.New(t)
			row, err := tc.rq.filterRowsByRegex(mockdata)

			if tc.expectedErr != nil {
				req.Equal(tc.expectedErr.Error(), err.Error())
			} else {
				req.NoError(err)
				req.Equal(tc.resultCount, len(row))

				for _, uq := range tc.unexpectedQualifiers {
					for rowKey, rowData := range row {
						if strings.HasPrefix(rowKey, tc.rq.rowKeyPrefix) {
							_, exists := rowData.Columns[tc.rq.family][uq]
							req.False(exists, "unexpected qualifier %s found in response", uq)
						}
					}
				}
			}
		})
	}
}

func Test_read(t *testing.T) {
	t.Parallel()
	now := time.Now()
	mockdata := &litetable.Data{
		"user:12345": {
			"profile": {
				"firstName": {
					{Value: []byte("John"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Smith"), Timestamp: now},
				},
				"email": {
					{Value: []byte("john@example.net"), Timestamp: now},
				},
			},
		},
		"user:12567": {
			"profile": {
				"firstName": {
					{Value: []byte("Ruby"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Rocket"), Timestamp: now},
				},
				"email": {
					{Value: []byte("ruby@example.net"), Timestamp: now},
				},
			},
		},
		"user:56789": {
			"profile": {
				"firstName": {
					{Value: []byte("Rosie"), Timestamp: now},
				},
				"lastName": {
					{Value: []byte("Rocket"), Timestamp: now},
				},
				"email": {
					{Value: []byte("rosie@example.net"), Timestamp: now},
				},
			},
		},
	}

	tests := map[string]struct {
		query []byte

		validateFamily bool
		familyAllowed  bool

		getData       bool
		mockData      *litetable.Data
		expectedError error
	}{
		"invalid read query": {
			query:         []byte("key=user:12345 family=profile qualifier=firstName latest=5 timestamp=2023-10-01"),
			expectedError: errors.New("invalid format: invalid timestamp format: 2023-10-01"),
		},
		"column family does not exist": {
			query:          []byte("key=user:12345 family=profile qualifier=firstName latest=5 timestamp=2023-10-01T12:00:00Z"),
			validateFamily: true,
			expectedError:  errors.New("column family does not exist: profile"),
		},
		"row key doesn't exist in data": {
			query:          []byte("key=user:99999 family=profile qualifier=firstName latest=5 timestamp=2023-10-01T12:00:00Z"),
			validateFamily: true,
			familyAllowed:  true,
			getData:        true,
			mockData: &litetable.Data{
				"user:12345": {},
			},
			expectedError: errors.New("row not found: user:99999"),
		},
		"rowKey exists in data": {
			query: []byte("key=user:12345 family=profile qualifier=firstName latest=5" +
				" timestamp=2023-10-01T12:00:00Z"),
			validateFamily: true,
			familyAllowed:  true,
			getData:        true,
			mockData:       mockdata,
		},
		"rowKeyPrefix exists in data": {
			query: []byte("prefix=user:12 family=profile qualifier=firstName latest=5" +
				" timestamp=2023-10-01T12:00:00Z"),
			validateFamily: true,
			familyAllowed:  true,
			getData:        true,
			mockData:       mockdata,
		},
		"rowKeyRegex exists in data": {
			query: []byte("regex=user:[0-9]+ family=profile qualifier=firstName latest=5" +
				" timestamp=2023-10-01T12:00:00Z"),
			validateFamily: true,
			familyAllowed:  true,
			getData:        true,
			mockData:       mockdata,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			req := require.New(t)

			mockStorage := NewMockshardManager(ctrl)

			if tc.validateFamily {
				mockStorage.
					EXPECT().
					IsFamilyAllowed(gomock.Any()).
					Return(tc.familyAllowed)
			}

			if tc.getData {
				// Handle different query types
				if strings.Contains(string(tc.query), "key=") {
					// For direct key lookup
					mockStorage.
						EXPECT().
						GetRowByFamily(gomock.Any(), gomock.Any()).
						Return(tc.mockData, true)
				} else if strings.Contains(string(tc.query), "prefix=") {
					// For prefix search
					mockStorage.
						EXPECT().
						FilterRowsByPrefix(gomock.Any()).
						Return(tc.mockData, len(*tc.mockData) > 0)
				} else if strings.Contains(string(tc.query), "regex=") {
					// For regex search
					mockStorage.
						EXPECT().
						FilterRowsByRegex(gomock.Any()).
						Return(tc.mockData, len(*tc.mockData) > 0)
				}
			}

			m := &Manager{
				shardStorage: mockStorage,
			}
			got, err := m.read(tc.query)

			if tc.expectedError != nil {
				req.Equal(tc.expectedError.Error(), err.Error())
			} else {
				req.NoError(err)
				req.NotNil(got)
			}
		})
	}
}
