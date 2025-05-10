package grpc

import (
	litetable2 "github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/pkg/proto"
)

type operations interface {
	Read(query string) (map[string]*litetable2.Row, error)
	Write(query string) (map[string]*litetable2.Row, error)
	Delete(query string) error
}

type litetable struct {
	proto.UnimplementedLitetableServiceServer
	operations operations
}
