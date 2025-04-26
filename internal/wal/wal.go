package wal

import (
	"db/internal/engine"
	"os"
	"sync"
	"time"
)

type Entry struct {
	Operation string                      `json:"operation"`
	RowKey    string                      `json:"row_key"`
	Columns   map[string]engine.Qualifier `json:"cols"`
	Timestamp time.Time                   `json:"timestamp"`
}

type Manager struct {
	mu      sync.RWMutex
	data    map[string]engine.Qualifier
	walFile *os.File
}
