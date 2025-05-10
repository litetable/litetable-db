package cdc_emitter

import (
	"encoding/json"
	"errors"
	"github.com/litetable/litetable-db/internal/litetable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"net"
	"sync"
	"testing"
	"time"
)

func TestManager_Emit(t *testing.T) {
	m := &Manager{
		emitChan: make(chan *CDCParams, 1),
	}

	params := &CDCParams{}
	m.Emit(params)

	emitted := <-m.emitChan
	if emitted != params {
		t.Errorf("Expected emitted params to be %v, got %v", params, emitted)
	}

	close(m.emitChan)
}

func TestManager_raiseCDCEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		params        *CDCParams
		clients       int
		writeErrors   []error
		expectRemoved []bool
	}{
		{
			name: "single client successful write",
			params: &CDCParams{
				Operation: litetable.OperationWrite,
				Family:    "user",
				Qualifier: "name",
				RowKey:    "user:123",
				Column: litetable.TimestampedValue{
					Value:     []byte("test-value"),
					Timestamp: time.Now().UnixNano(),
				},
			},
			clients:       1,
			writeErrors:   []error{nil},
			expectRemoved: []bool{false},
		},
		{
			name: "multiple clients successful write",
			params: &CDCParams{
				Operation: litetable.OperationWrite,
				Family:    "user",
				Qualifier: "name",
				RowKey:    "user:456",
				Column: litetable.TimestampedValue{
					Value:     []byte("updated-value"),
					Timestamp: time.Now().UnixNano(),
				},
			},
			clients:       3,
			writeErrors:   []error{nil, nil, nil},
			expectRemoved: []bool{false, false, false},
		},
		{
			name: "some clients with write errors",
			params: &CDCParams{
				Operation: litetable.OperationDelete,
				RowKey:    "user:789",
				Family:    "user",
				Qualifier: "name",
				Column: litetable.TimestampedValue{
					Value:     []byte("deleted-value"),
					Timestamp: time.Now().UnixNano(),
				},
			},
			clients:       3,
			writeErrors:   []error{nil, errors.New("write error"), nil},
			expectRemoved: []bool{false, true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Create manager
			m := &Manager{
				clients:    make(map[net.Conn]bool),
				clientsMux: sync.Mutex{},
			}

			// Create the expected event from CDCParams
			expectedEvent := &event{
				Operation:   tt.params.Operation,
				RowKey:      tt.params.RowKey,
				Family:      tt.params.Family,
				Qualifier:   tt.params.Qualifier,
				Value:       tt.params.Column.Value,
				Timestamp:   tt.params.Column.Timestamp,
				IsTombstone: tt.params.Column.IsTombstone,
			}
			if tt.params.Column.ExpiresAt != nil {
				expectedEvent.ExpiresAt = tt.params.Column.ExpiresAt
			}

			// Marshal the expected event instead of CDCParams
			expectedData, err := json.Marshal(expectedEvent)
			require.NoError(t, err)
			expectedMessage := append(expectedData, '\n')

			// Rest of the test remains the same
			// Create mock connections
			mockConns := make([]net.Conn, tt.clients)
			for i := 0; i < tt.clients; i++ {
				mockConn := NewMockConn(ctrl)
				// Add to clients map
				m.clients[mockConn] = true
				mockConns[i] = mockConn
				// Set write deadline expectation
				mockConn.EXPECT().SetWriteDeadline(gomock.Any()).Return(nil)
				// Set write expectation with appropriate error
				mockConn.EXPECT().Write(gomock.Eq(expectedMessage)).Return(len(expectedMessage), tt.writeErrors[i])
				// If error, expect Close to be called
				if tt.writeErrors[i] != nil {
					mockConn.EXPECT().Close().Return(nil)
				}
			}

			// Call the method being tested
			m.raiseCDCEvent(tt.params)

			// Verify clients are in the expected state
			for i, conn := range mockConns {
				_, exists := m.clients[conn]
				assert.Equal(t, !tt.expectRemoved[i], exists,
					"Client %d should be %s", i,
					map[bool]string{true: "removed", false: "present"}[tt.expectRemoved[i]])
			}
		})
	}
}
