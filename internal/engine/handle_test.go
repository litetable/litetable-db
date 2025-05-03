package engine

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestEngine_Handle(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		closeErr   error
		readResult int
		readErr    error

		opsResult []byte
		opsErr    error

		shouldSucceed bool
		writeErr      error
	}{
		"read failure": {
			readResult: 0,
			readErr:    assert.AnError,
		},
		"ops failure": {
			opsResult: nil,
			opsErr:    assert.AnError,
		},
		"successful write": {
			opsResult:     []byte("Great!"),
			shouldSucceed: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockConn := NewMockConn(ctrl)
			mockOps := NewMockops(ctrl)

			req := require.New(t)
			// all calls call Read(), Write and Close()
			mockConn.
				EXPECT().
				Read(gomock.Any()).
				Return(tc.readResult, tc.readErr).
				MaxTimes(1)

			mockConn.
				EXPECT().
				Write(gomock.Any()).
				DoAndReturn(func(b []byte) (n int, err error) {
					if tc.readErr != nil {
						req.Equal("Error: "+tc.readErr.Error(), string(b))
					}
					if tc.opsErr != nil {
						req.Equal("Error: "+tc.opsErr.Error(), string(b))
					}

					if tc.shouldSucceed {
						req.Equal(string(tc.opsResult), string(b))
					}
					return 0, tc.writeErr
				}).
				MaxTimes(1)

			mockOps.
				EXPECT().
				Run(gomock.Any()).
				Return(tc.opsResult, tc.opsErr).
				MaxTimes(1)

			mockConn.
				EXPECT().
				Close().
				Return(tc.closeErr).
				MaxTimes(1)

			e := &Engine{
				maxBufferSize: 4096,
				operations:    mockOps,
			}

			e.Handle(mockConn)
		})
	}
}
