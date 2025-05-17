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

func TestLt_CreateFamily(t *testing.T) {
	tests := map[string]struct {
		request         *proto.CreateFamilyRequest
		mockSetup       func(m *Mockoperations)
		expectedCode    codes.Code
		expectedMessage string
	}{
		"missing family field": {
			request: &proto.CreateFamilyRequest{Family: []string{}},
			mockSetup: func(m *Mockoperations) {
				// No call expected
			},
			expectedCode:    codes.InvalidArgument,
			expectedMessage: "family required",
		},
		"internal error from CreateFamilies": {
			request: &proto.CreateFamilyRequest{Family: []string{"testFamily"}},
			mockSetup: func(m *Mockoperations) {
				m.EXPECT().
					CreateFamilies([]string{"testFamily"}).
					Return(errors.New("backend error"))
			},
			expectedCode:    codes.Internal,
			expectedMessage: "failed to create family: backend error",
		},
		"successful request": {
			request: &proto.CreateFamilyRequest{Family: []string{"validFamily"}},
			mockSetup: func(m *Mockoperations) {
				m.EXPECT().
					CreateFamilies([]string{"validFamily"}).
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

			resp, err := svc.CreateFamily(context.Background(), tc.request)

			if tc.expectedCode == codes.OK {
				req.NoError(err)
				req.Nil(resp)
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
