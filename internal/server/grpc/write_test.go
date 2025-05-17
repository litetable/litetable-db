package grpc

import (
	"context"
	"errors"
	litetable2 "github.com/litetable/litetable-db/internal/litetable"
	"github.com/litetable/litetable-db/pkg/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestLt_Write(t *testing.T) {
	tests := map[string]struct {
		request         *proto.WriteRequest
		expectedQuery   string
		mockSetup       func(m *Mockoperations)
		expectedCode    codes.Code
		expectedMessage string
	}{
		"missing required fields": {
			request: &proto.WriteRequest{},
			mockSetup: func(m *Mockoperations) {
				// no call expected
			},
			expectedCode:    codes.InvalidArgument,
			expectedMessage: "family required",
		},
		"internal error from Write": {
			request: &proto.WriteRequest{
				Family: "f1",
				RowKey: "r1",
				Qualifiers: []*proto.ColumnQualifier{
					{Name: "q1", Value: []byte("v1")},
				},
			},
			expectedQuery: "family=f1 key=r1 qualifier=q1 value=v1",
			mockSetup: func(m *Mockoperations) {
				// URL encoding of "v1" = "v1" (no special chars)
				m.EXPECT().
					Write("family=f1 key=r1 qualifier=q1 value=v1").
					Return(nil, errors.New("db down"))
			},
			expectedCode:    codes.Internal,
			expectedMessage: "failed to write data: db down",
		},
		"successful write with encoded value": {
			request: &proto.WriteRequest{
				Family: "f2",
				RowKey: "r2",
				Qualifiers: []*proto.ColumnQualifier{
					{Name: "q2", Value: []byte("hello world!")}, // space & bang will be encoded
				},
			},
			expectedQuery: "family=f2 key=r2 qualifier=q2 value=hello+world%21",
			mockSetup: func(m *Mockoperations) {
				m.EXPECT().
					Write("family=f2 key=r2 qualifier=q2 value=hello+world%21").
					Return(map[string]*litetable2.Row{
						"r2": {
							Key: "r2",
							Columns: map[string]litetable2.VersionedQualifier{
								"f2": {
									"q2": {{Value: []byte("hello world!"), Timestamp: 123456}},
								},
							},
						},
					}, nil)
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

			resp, err := svc.Write(context.Background(), tc.request)

			if tc.expectedCode == codes.OK {
				req.NoError(err)
				req.NotNil(resp)
				row, ok := resp.Rows["r2"]
				req.True(ok)
				req.Equal("r2", row.Key)
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
