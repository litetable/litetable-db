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

func TestLt_Read(t *testing.T) {
	tests := map[string]struct {
		request         *proto.ReadRequest
		expectedQuery   string
		mockSetup       func(m *Mockoperations)
		expectedCode    codes.Code
		expectedMessage string
	}{
		"missing family and rowKey": {
			request: &proto.ReadRequest{},
			mockSetup: func(m *Mockoperations) {
				// no op
			},
			expectedCode:    codes.InvalidArgument,
			expectedMessage: "family required",
		},
		"internal error from operations.Read": {
			request: &proto.ReadRequest{
				Family:    "fam",
				RowKey:    "key1",
				QueryType: proto.QueryType_EXACT,
			},
			expectedQuery: "family=fam key=key1",
			mockSetup: func(m *Mockoperations) {
				m.EXPECT().
					Read("family=fam key=key1").
					Return(nil, errors.New("boom"))
			},
			expectedCode:    codes.Internal,
			expectedMessage: "failed to read data: boom",
		},
		"successful read with qualifiers and latest": {
			request: &proto.ReadRequest{
				Family:     "fam",
				RowKey:     "r1",
				QueryType:  proto.QueryType_PREFIX,
				Qualifiers: []string{"a", "b"},
				Latest:     2,
			},
			expectedQuery: "family=fam prefix=r1 qualifier=a qualifier=b latest=2",
			mockSetup: func(m *Mockoperations) {
				m.EXPECT().
					Read("family=fam prefix=r1 qualifier=a qualifier=b latest=2").
					Return(map[string]*litetable2.Row{
						"r1": {
							Key: "r1",
							Columns: map[string]litetable2.VersionedQualifier{
								"fam": {
									"a": {{Value: []byte("v1"), Timestamp: 1111}},
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

			resp, err := svc.Read(context.Background(), tc.request)

			if tc.expectedCode == codes.OK {
				req.NoError(err)
				req.NotNil(resp)
				row, ok := resp.Rows["r1"]
				req.True(ok)
				req.Equal("r1", row.Key)
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
