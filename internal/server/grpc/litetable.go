package grpc

import (
	"errors"
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
