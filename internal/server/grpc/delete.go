package grpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/pkg/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (l *litetable) validateDelete(msg *proto.DeleteRequest) error {
	var errGrp []error
	if msg.GetFamily() == "" {
		errGrp = append(errGrp, status.Errorf(codes.InvalidArgument, "family required"))
	}
	if msg.GetRowKey() == "" {
		errGrp = append(errGrp, status.Errorf(codes.InvalidArgument, "rowKey required"))
	}

	return errors.Join(errGrp...)
}

func (l *litetable) Delete(ctx context.Context, msg *proto.DeleteRequest) (*proto.Empty, error) {
	if err := l.validateDelete(msg); err != nil {
		return nil, err
	}

	// Ex: DELETE family="family" rowKey="rowKey" qualifier="qualifier"
	queryStr := "key=" + msg.GetRowKey()

	if msg.GetFamily() != "" {
		queryStr += " family=" + msg.GetFamily()
	}

	for _, qualifier := range msg.GetQualifiers() {
		queryStr += " qualifier=" + qualifier
	}

	// The timestamp signals where we should place the tombstone
	fromTS := msg.GetTimestampUnix()
	if fromTS > 0 {
		queryStr += " timestamp=" + fmt.Sprintf("%d", fromTS)
	}

	// TTL is expected to be a int32 that ='s the number of seconds till garbage collection
	ttl := msg.GetTtl()
	if ttl > 0 {
		queryStr += " ttl=" + fmt.Sprintf("%d", ttl)
	}

	if err := l.operations.Delete(queryStr); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete data: %v", err)
	}
	return &proto.Empty{}, nil
}
