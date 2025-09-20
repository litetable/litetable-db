package grpc

import (
	"context"
	"net"

	litetable2 "github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/pkg/proto"
)

//go:generate mockgen -destination=./litetable_mock.go -package=grpc -source=./litetable.go

type operations interface {
	CreateFamilies(ctx context.Context, families []string) error
	Read(ctx context.Context, query string) (map[string]*litetable2.Row, error)
	Write(ctx context.Context, query string) (map[string]*litetable2.Row, error)
	Delete(ctx context.Context, query string) error
}

type grpcServer interface {
	Serve(lis net.Listener) error
	GracefulStop()
}

type lt struct {
	proto.UnimplementedLitetableServiceServer
	operations operations
}
