package grpc

import (
	"context"
	"errors"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/url"
	"time"
)

func (l *litetable) validateWrite(msg *proto.WriteRequest) error {
	var errGrp []error
	if msg.GetFamily() == "" {
		errGrp = append(errGrp, status.Errorf(codes.InvalidArgument, "family required"))
	}
	if msg.GetRowKey() == "" {
		errGrp = append(errGrp, status.Errorf(codes.InvalidArgument, "rowKey required"))
	}
	if len(msg.GetQualifiers()) == 0 {
		errGrp = append(errGrp, status.Errorf(codes.InvalidArgument, "qualifiers required"))
	}
	return errors.Join(errGrp...)
}

func (l *litetable) Write(ctx context.Context, msg *proto.WriteRequest) (*proto.LitetableData,
	error) {
	if err := l.validateWrite(msg); err != nil {
		return nil, err
	}
	now := time.Now()
	log.Debug().Msgf("Write request: %v", msg)
	// Ex: WRITE family="family" rowKey="rowKey" qualifier="qualifier" value="value"
	queryStr := "family=" + msg.GetFamily()
	queryStr += " key=" + msg.GetRowKey()
	for _, qualifier := range msg.GetQualifiers() {
		queryStr += " qualifier=" + qualifier.GetName()
		if len(qualifier.GetValue()) > 0 {
			// URL encode binary values to preserve all bytes properly
			encodedValue := url.QueryEscape(string(qualifier.GetValue()))
			queryStr += " value=" + encodedValue
		}
	}

	result, err := l.operations.Write(queryStr)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write data: %v", err)
	}

	log.Debug().Msgf("Write latest: %v", time.Since(now))
	return convertToProtoData(result), nil
}
