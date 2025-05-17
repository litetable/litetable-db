package grpc

import (
	litetable2 "github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConvertToProtoData(t *testing.T) {
	tests := map[string]struct {
		input    map[string]*litetable2.Row
		expected *proto.LitetableData
	}{
		"empty input": {
			input:    map[string]*litetable2.Row{},
			expected: &proto.LitetableData{Rows: map[string]*proto.Row{}},
		},
		"single row with one family and one qualifier": {
			input: map[string]*litetable2.Row{
				"row1": {
					Key: "row1",
					Columns: map[string]litetable2.VersionedQualifier{
						"family1": {
							"qualifier1": {
								{Value: []byte("v1"), Timestamp: 1000},
							},
						},
					},
				},
			},
			expected: &proto.LitetableData{
				Rows: map[string]*proto.Row{
					"row1": {
						Key: "row1",
						Cols: map[string]*proto.VersionedQualifier{
							"family1": {
								Qualifiers: map[string]*proto.QualifierValues{
									"qualifier1": {
										Values: []*proto.TimestampedValue{
											{
												Value:         []byte("v1"),
												TimestampUnix: 1000,
												ExpiresAtUnix: 0,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"row with multiple qualifiers and expiresAt": {
			input: map[string]*litetable2.Row{
				"row2": {
					Key: "row2",
					Columns: map[string]litetable2.VersionedQualifier{
						"family2": {
							"a": {
								{Value: []byte("one"), Timestamp: 2000, ExpiresAt: 5000},
							},
							"b": {
								{Value: []byte("two"), Timestamp: 3000, ExpiresAt: 6000},
							},
						},
					},
				},
			},
			expected: &proto.LitetableData{
				Rows: map[string]*proto.Row{
					"row2": {
						Key: "row2",
						Cols: map[string]*proto.VersionedQualifier{
							"family2": {
								Qualifiers: map[string]*proto.QualifierValues{
									"a": {
										Values: []*proto.TimestampedValue{
											{Value: []byte("one"), TimestampUnix: 2000},
										},
									},
									"b": {
										Values: []*proto.TimestampedValue{
											{Value: []byte("two"), TimestampUnix: 3000},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := convertToProtoData(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}
