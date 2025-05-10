package grpc

import (
	"context"
	"github.com/litetable/litetable-db/pkg/proto"
)

func (l *litetable) Write(ctx context.Context, msg *proto.WriteRequest) (*proto.LitetableData,
	error) {
	return nil, nil
}
