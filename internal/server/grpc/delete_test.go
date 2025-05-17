package grpc

import (
	"context"
	"errors"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestLt_Delete(t *testing.T) {
	tests := map[string]struct {
		request         *proto.DeleteRequest
		mockSetup       func(m *Mockoperations)
		expectedCode    codes.Code
		expectedMessage string
	}{
		"missing rowKey and family": {
			request: &proto.DeleteRequest{},
			mockSetup: func(m *Mockoperations) {
				// No call expected
			},
			expectedCode:    codes.InvalidArgument,
			expectedMessage: "rowKey required",
		},
		"internal error from Delete": {
			request: &proto.DeleteRequest{
				Family:     "fam",
				RowKey:     "rk",
				Qualifiers: []string{"q1"},
			},
			mockSetup: func(m *Mockoperations) {
				// Expected query: key=rk family=fam qualifier=q1
				m.EXPECT().
					Delete("key=rk family=fam qualifier=q1").
					Return(errors.New("boom"))
			},
			expectedCode:    codes.Internal,
			expectedMessage: "failed to delete data: boom",
		},
		"successful delete with full params": {
			request: &proto.DeleteRequest{
				Family:        "fam",
				RowKey:        "rk",
				Qualifiers:    []string{"q1", "q2"},
				TimestampUnix: 12345,
				Ttl:           60,
			},
			mockSetup: func(m *Mockoperations) {
				m.EXPECT().
					Delete("key=rk family=fam qualifier=q1 qualifier=q2 timestamp=12345 ttl=60").
					Return(nil)
			},
			expectedCode:    codes.OK,
			expectedMessage: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := require.New(t)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockOps := NewMockoperations(ctrl)
			if tc.mockSetup != nil {
				tc.mockSetup(mockOps)
			}

			svc := &lt{
				operations: mockOps,
			}

			resp, err := svc.Delete(context.Background(), tc.request)

			if tc.expectedCode == codes.OK {
				req.NoError(err)
				req.NotNil(resp)
			} else {
				req.Error(err)
				st, ok := status.FromError(err)
				req.True(ok)
				req.Equal(tc.expectedCode, st.Code())
				req.Contains(st.Message(), tc.expectedMessage)
			}
		})
	}
}
