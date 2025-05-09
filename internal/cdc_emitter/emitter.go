package cdc_emitter

import (
	"encoding/json"
	"fmt"
	"github.com/litetable/litetable-db/internal/litetable"
	"time"
)

type CDCParams struct {
	Operation litetable.Operation        `json:"operation"`
	Family    string                     `json:"family"`
	RowKey    string                     `json:"rowKey"`
	Qualifier string                     `json:"qualifier"`
	Column    litetable.TimestampedValue `json:"column"`
}

type event struct {
	Operation   litetable.Operation `json:"operation"`
	RowKey      string              `json:"key"`
	Family      string              `json:"family"`
	Qualifier   string              `json:"qualifier"`
	Value       []byte              `json:"value"`
	Timestamp   time.Time           `json:"timestamp"`
	IsTombstone bool                `json:"isTombstone"`
	ExpiresAt   *time.Time          `json:"expiresAt"`
}

// Emit pushes a CDC event to the channel. This is how consumers get notifier
// of a CDC event.
func (m *Manager) Emit(params *CDCParams) {
	// Emit the CDC event
	m.emitChan <- params
}

// raiseCDCEvent will emit the CDC event to all connected clients.
func (m *Manager) raiseCDCEvent(params *CDCParams) {
	e := buildCDCEvent(params)
	// Convert params to JSON
	d, err := json.Marshal(e)
	if err != nil {
		fmt.Printf("Failed to marshal CDC event: %v\n", err)
		return
	}

	// Add newline for message framing
	msg := append(d, '\n')

	// no new clients while writing
	m.clientsMux.Lock()
	defer m.clientsMux.Unlock()

	for client := range m.clients {
		// Non-blocking write with short timeout
		_ = client.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, err = client.Write(msg)
		if err != nil {
			_ = client.Close()
			delete(m.clients, client)
		}
	}
}

func buildCDCEvent(p *CDCParams) *event {
	e := &event{
		Operation:   p.Operation,
		RowKey:      p.RowKey,
		Family:      p.Family,
		Qualifier:   p.Qualifier,
		Value:       p.Column.Value,
		Timestamp:   p.Column.Timestamp,
		IsTombstone: p.Column.IsTombstone,
		ExpiresAt:   p.Column.ExpiresAt,
	}
	return e
}
