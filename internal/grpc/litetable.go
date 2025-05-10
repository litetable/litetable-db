package grpc

import (
	"context"
	"errors"
	"fmt"
	litetable2 "github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/pkg/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type operations interface {
	Read(query string) (map[string]*litetable2.Row, error)
}

type litetable struct {
	proto.UnimplementedLitetableServiceServer
	operations operations
}

func (l *litetable) validateRead(msg *proto.ReadRequest) error {
	var errGrp []error
	if msg.GetFamily() == "" {
		errGrp = append(errGrp, status.Errorf(codes.InvalidArgument, "family required"))
	}
	if msg.GetRowKey() == "" {
		errGrp = append(errGrp, status.Errorf(codes.InvalidArgument, "rowKey required"))
	}

	return errors.Join(errGrp...)
}

func (l *litetable) Read(ctx context.Context, msg *proto.ReadRequest) (*proto.LitetableData,
	error) {
	if err := l.validateRead(msg); err != nil {
		return nil, err
	}

	// Ex: READ family="family" rowKey="rowKey" qualifier="qualifier" latest=5
	// queryStr := "READ"
	queryStr := "family=" + msg.GetFamily()
	if msg.GetQueryType() == proto.QueryType_EXACT {
		queryStr += " key=" + msg.GetRowKey()
	}

	if len(msg.GetQualifiers()) > 0 {
		for _, qualifier := range msg.GetQualifiers() {
			queryStr += " qualifier=" + qualifier
		}
	}

	if msg.GetLatest() > 0 {
		queryStr += fmt.Sprintf(" latest=%d", msg.GetLatest())
	}

	result, err := l.operations.Read(queryStr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read data: %v", err)
	}

	return convertToProtoData(result), nil
}

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
						TimestampUnix: tv.Timestamp.UnixNano(),
					}
					
					if tv.ExpiresAt != nil {
						protoTv.ExpiresAtUnix = tv.ExpiresAt.UnixNano()
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
