package grpc

import (
	"context"
	"errors"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

func (l *lt) validateCreateFamilyRequest(msg *proto.CreateFamilyRequest) error {
	var errGrp []error

	families := msg.GetFamily()
	if len(families) == 0 {
		errGrp = append(errGrp, status.Errorf(codes.InvalidArgument, "family required"))
	}

	return errors.Join(errGrp...)
}

func (l *lt) CreateFamily(ctx context.Context, msg *proto.CreateFamilyRequest) (*proto.
	Empty, error) {
	start := time.Now()
	if err := l.validateCreateFamilyRequest(msg); err != nil {
		return nil, err
	}

	log.Debug().Msgf("CreateFamily request: %v", msg)

	if err := l.operations.CreateFamilies(msg.GetFamily()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create family: %v", err)
	}
	log.Debug().Msgf("CreateFamily successful: %v", time.Since(start))
	return nil, nil
}
