package grpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

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
	now := time.Now()
	log.Debug().Msgf("Read request: %v", msg)
	if err := l.validateRead(msg); err != nil {
		return nil, err
	}

	// Ex: READ family="family" rowKey="rowKey" qualifier="qualifier" latest=5
	queryStr := "family=" + msg.GetFamily()
	if msg.GetQueryType() == proto.QueryType_EXACT {
		queryStr += " key=" + msg.GetRowKey()
	}

	if msg.GetQueryType() == proto.QueryType_PREFIX {
		queryStr += " prefix=" + msg.GetRowKey()
	}

	if msg.GetQueryType() == proto.QueryType_REGEX {
		queryStr += " regex=" + msg.GetRowKey()
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

	log.Debug().Msgf("Read latency: %v", time.Since(now))
	return convertToProtoData(result), nil
}
