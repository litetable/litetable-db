package grpc

import (
	litetable2 "github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/pkg/proto"
)

func convertToProtoData(rows map[string]*litetable2.Row) *proto.LitetableData {
	protoData := &proto.LitetableData{
		Rows: make(map[string]*proto.Row),
	}

	for rowKey, row := range rows {
		protoRow := &proto.Row{
			Key:  row.Key,
			Cols: make(map[string]*proto.VersionedQualifier),
		}

		for familyName, versionedQualifiers := range row.Columns {
			columnFamily := &proto.VersionedQualifier{
				Qualifiers: make(map[string]*proto.QualifierValues),
			}

			for qualifierName, timestampedValues := range versionedQualifiers {
				qualifierValues := &proto.QualifierValues{
					Values: make([]*proto.TimestampedValue, 0, len(timestampedValues)),
				}

				for _, tv := range timestampedValues {
					protoTv := &proto.TimestampedValue{
						Value:         tv.Value,
						TimestampUnix: tv.Timestamp,
					}

					if tv.ExpiresAt == 0 {
						protoTv.ExpiresAtUnix = tv.ExpiresAt
					}

					qualifierValues.Values = append(qualifierValues.Values, protoTv)
				}

				columnFamily.Qualifiers[qualifierName] = qualifierValues
			}

			protoRow.Cols[familyName] = columnFamily
		}

		protoData.Rows[rowKey] = protoRow
	}

	return protoData
}
