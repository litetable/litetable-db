package cdc_emitter

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

//go:generate mockgen -destination=conn_mock.go -package=cdc_emitter net Conn

func TestNew(t *testing.T) {
	t.Parallel()

	req := require.New(t)
	t.Run("invalid config", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{}
		engine, err := New(cfg)
		req.Error(err)
		req.Nil(engine)
	})

	t.Run("valid config", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			Port:    9999,
			Address: "127.0.0.1",
		}
		engine, err := New(cfg)
		req.NoError(err)
		req.NotNil(engine)
	})

	t.Run("Test Name", func(t *testing.T) {
		m := &Manager{}

		require.Equal(t, "CDC Emitter", m.Name())
	})

	t.Run("Test Stop", func(t *testing.T) {
		m := &Manager{}
		err := m.Stop()
		assert.Nil(t, err)
	})
}

func TestManager_Handle(t *testing.T) {
	tests := []struct {
		name             string
		readErr          error
		shouldDisconnect bool
	}{
		{
			name:             "client remains connected",
			readErr:          nil,
			shouldDisconnect: false,
		},
		{
			name:             "client disconnects with EOF",
			readErr:          io.EOF,
			shouldDisconnect: true,
		},
		{
			name:             "client disconnects with error",
			readErr:          errors.New("connection reset"),
			shouldDisconnect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Create mock connection
			mockConn := NewMockConn(ctrl)

			// Create mock address
			mockAddr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}

			// Setup RemoteAddr for logging
			mockConn.EXPECT().RemoteAddr().Return(mockAddr).AnyTimes()

			// Setup Close - this will be called when the connection ends
			mockConn.EXPECT().Close().Return(nil).AnyTimes()

			// Create a context with timeout to ensure test completes
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			// Create manager
			m := &Manager{
				clients:    make(map[net.Conn]bool),
				clientsMux: sync.Mutex{},
				procCtx:    context.Background(),
			}

			// Register the client first
			m.clientsMux.Lock()
			m.clients[mockConn] = true
			m.clientsMux.Unlock()

			// Setup Read expectations
			if tt.shouldDisconnect {
				// For disconnect tests, return error on first read
				mockConn.EXPECT().Read(gomock.Any()).Return(0, tt.readErr).Times(1)
			} else {
				// For connected test case, first set up a read that blocks until test completes
				mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(
					func(b []byte) (int, error) {
						// Wait until context is done, then return EOF to terminate the loop
						<-ctx.Done()
						return 0, io.EOF
					},
				)
			}

			// Run handle in goroutine
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.handle(mockConn)
			}()

			// Test case specific expectations
			if tt.shouldDisconnect {
				// For disconnection tests, wait for handle to complete
				wg.Wait()

				// Verify client was removed
				m.clientsMux.Lock()
				_, exists := m.clients[mockConn]
				m.clientsMux.Unlock()
				assert.False(t, exists, "Client should be removed after disconnection")
			} else {
				// For the "remains connected" test, wait for timeout then check client is registered
				<-ctx.Done()

				// Verify client was registered and remains in the map
				m.clientsMux.Lock()
				_, exists := m.clients[mockConn]
				m.clientsMux.Unlock()
				assert.True(t, exists, "Client should be registered in connected state")

				// Wait for handle to complete (since we return EOF after ctx is done)
				wg.Wait()
			}
		})
	}
}
