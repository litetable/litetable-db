package server

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	tests := map[string]struct {
		cfg   *Config
		want  *Server
		error error
	}{
		"invalid config": {
			cfg:   &Config{},
			error: errors.New("address is required\nport must be between 1 and 65535"),
		},
		"valid config": {
			cfg: &Config{
				Address: "localhost",
				Port:    8080,
			},
			want: &Server{
				address: "localhost",
				port:    8080,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := New(tc.cfg)
			req := require.New(t)

			if tc.error != nil {
				req.Error(err)
				req.Equal(tc.error.Error(), err.Error())
				return
			}

			req.NoError(err)
			req.Equal(tc.want.port, got.port)
			req.Equal(tc.want.address, got.address)
			req.NotNil(got.server)
		})
	}
}

func TestServer_Name(t *testing.T) {
	s := &Server{}
	got := s.Name()
	assert.Equal(t, "LiteTable http server", got)
}

func TestServer_Start(t *testing.T) {
	tests := map[string]struct {
		shouldCallListen bool
		listenErr        error
		shouldFail       bool
	}{
		"unsuccessful start": {
			shouldCallListen: true,
			listenErr:        errors.New("bind error"),
			shouldFail:       true,
		},
		"graceful shutdown on start will return err": {
			shouldCallListen: true,
			listenErr:        http.ErrServerClosed,
			shouldFail:       true,
		},
		"successful start": {
			shouldCallListen: true,
			listenErr:        nil,
			shouldFail:       false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockServer := NewMockhttpServer(ctrl)
			mockServer.EXPECT().Addr().Return("localhost:8080")

			if tc.shouldCallListen {
				mockServer.EXPECT().ListenAndServe().Return(tc.listenErr)
			}

			server := &Server{server: mockServer}
			err := server.Start()

			req := require.New(t)
			if tc.shouldFail {
				req.Error(err)
			} else {
				req.NoError(err)
			}
		})
	}

	t.Run("real server: /health endpoint returns 200", func(t *testing.T) {
		t.Parallel()
		req := require.New(t)

		// Pick an available port by binding to :0
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		req.NoError(err)
		defer listener.Close()

		port := 10375 // seems random enough to always work :smile:
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		// Setup the server
		cfg := &Config{
			Address: "127.0.0.1",
			Port:    port,
		}
		srv, err := New(cfg)
		req.NoError(err)

		// Start the server
		err = srv.Start()
		req.NoError(err)

		// Give it a moment to start
		time.Sleep(100 * time.Millisecond)

		// Send request
		resp, err := http.Get(fmt.Sprintf("http://%s/health", addr))
		req.NoError(err)
		defer resp.Body.Close()

		// Read body
		body, err := io.ReadAll(resp.Body)
		req.NoError(err)

		// Assert
		req.Equal(http.StatusOK, resp.StatusCode)
		req.JSONEq(`{"status": "ok"}`, string(body))

		// Shutdown
		err = srv.Stop()
		req.NoError(err)
	})
}

func TestServer_Stop(t *testing.T) {
	tests := map[string]struct {
		shutDownErr error
	}{
		"failure during shutdown": {
			shutDownErr: assert.AnError,
		},
		"successful shutdown": {},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockServer := NewMockhttpServer(ctrl)
			mockServer.EXPECT().Shutdown(gomock.Any()).Return(tc.shutDownErr).Times(1)

			server := &Server{server: mockServer}
			err := server.Stop()

			req := require.New(t)
			if tc.shutDownErr != nil {
				req.Error(err)
				req.Contains(err.Error(), tc.shutDownErr.Error())
			} else {
				req.NoError(err)
			}
		})
	}
}
