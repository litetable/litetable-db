package cdc_emitter

import (
	"encoding/json"
	"fmt"
	"time"
)

type CDCParams struct {
	Msg string `json:"message"`
}

// Emit pushes a CDC event to the channel. This is how consumers get notifier
// of a CDC event.
func (m *Manager) Emit(params *CDCParams) {
	// Emit the CDC event
	m.emitChan <- params
}

// raiseCDCEvent will emit the CDC event to all connected clients.
func (m *Manager) raiseCDCEvent(params *CDCParams) {
	// Convert params to JSON
	data, err := json.Marshal(params)
	if err != nil {
		fmt.Printf("Failed to marshal CDC event: %v\n", err)
		return
	}

	// Add newline for message framing
	message := append(data, '\n')

	// no new clients while writing
	m.clientsMux.Lock()
	defer m.clientsMux.Unlock()

	for client := range m.clients {
		// Non-blocking write with short timeout
		_ = client.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, err = client.Write(message)
		if err != nil {
			_ = client.Close()
			delete(m.clients, client)
		}
	}
}
