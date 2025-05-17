package grpc

import (
	litetable2 "github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/pkg/proto"
	"net"
)

//go:generate mockgen -destination=./litetable_mock.go -package=grpc -source=./litetable.go

type operations interface {
	CreateFamilies(families []string) error
	Read(query string) (map[string]*litetable2.Row, error)
	Write(query string) (map[string]*litetable2.Row, error)
	Delete(query string) error
}

type grpcServer interface {
	Serve(lis net.Listener) error
	GracefulStop()
}

type lt struct {
	proto.UnimplementedLitetableServiceServer
	operations operations
}
