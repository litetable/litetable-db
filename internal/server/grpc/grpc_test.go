package grpc

import (
	"context"
	"errors"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"net"
	"testing"
	"time"
)

func TestNewServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tests := map[string]struct {
		cfg   *Config
		error error
		want  *Server
	}{
		"invalid config": {
			cfg:   &Config{},
			error: errors.New("address required\nport required\noperations required"),
		},
		"valid config": {
			cfg: &Config{
				Address:    "127.0.0.1",
				Port:       8080,
				Operations: NewMockoperations(ctrl),
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := NewServer(test.cfg)
			req := require.New(t)
			if test.error != nil {
				req.Error(err)
				req.Nil(got)

				req.Equal(test.error.Error(), err.Error())
				return
			}

			req.NoError(err)
			req.NotNil(got)
		})
	}
}

func TestServer_Name(t *testing.T) {
	s := &Server{}
	require.Equal(t, "gRPC Server", s.Name())
}

func TestGRPCServer_Real(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockOps := NewMockoperations(ctrl)
	mockOps.EXPECT().
		CreateFamilies([]string{"testFamily"}).
		Return(nil)

	// bind to a free port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	srv := grpc.NewServer()
	proto.RegisterLitetableServiceServer(srv, &lt{
		operations: mockOps,
	})
	reflection.Register(srv)

	// Run server in background
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Error().Err(err).Msg("gRPC server error")
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	conn, err := grpc.Dial(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := proto.NewLitetableServiceClient(conn)

	// Make the real call
	resp, err := client.CreateFamily(context.Background(), &proto.CreateFamilyRequest{
		Family: []string{"testFamily"},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)

	srv.GracefulStop()
}

func TestServer_Start(t *testing.T) {
	t.Run("successful start", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockServer := NewMockgrpcServer(ctrl)
		ml := &mockListener{}

		mockServer.EXPECT().
			Serve(ml).
			DoAndReturn(func(net.Listener) error {
				// Simulate blocking serve
				time.Sleep(100 * time.Millisecond)
				return nil
			})

		s := &Server{
			address:  "127.0.0.1",
			port:     12345,
			server:   mockServer,
			listener: ml,
		}

		err := s.Start()
		require.NoError(t, err)
	})

	t.Run("serve error on start", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockServer := NewMockgrpcServer(ctrl)
		ml := &mockListener{}

		mockServer.EXPECT().
			Serve(ml).
			Return(errors.New("bind error"))

		s := &Server{
			address:  "127.0.0.1",
			port:     12345,
			server:   mockServer,
			listener: ml,
		}

		err := s.Start()
		require.Error(t, err)
		require.Contains(t, err.Error(), "bind error")
	})
}

func TestServer_Stop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockServer := NewMockgrpcServer(ctrl)
	mockServer.EXPECT().GracefulStop().Times(1)

	s := &Server{
		server: mockServer,
	}

	require.NoError(t, s.Stop())
}

type mockListener struct {
	net.Listener
}

func (m *mockListener) Accept() (net.Conn, error) { return nil, nil }
func (m *mockListener) Close() error              { return nil }
func (m *mockListener) Addr() net.Addr            { return &net.TCPAddr{Port: 12345} }
