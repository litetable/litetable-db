package v1

import (
	"github.com/litetable/litetable-db/internal/litetable"
)

type CDCEvent struct {
	Operation   litetable.Operation `json:"operation"`
	RowKey      string              `json:"key"`
	Family      string              `json:"family"`
	Qualifier   string              `json:"qualifier"`
	Value       []byte              `json:"value"`
	Timestamp   int64               `json:"timestamp"`
	IsTombstone bool                `json:"isTombstone"`
	ExpiresAt   int64               `json:"expiresAt"`
}
