package auth

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ctxKey struct{}

var (
	authorizationKey = "authorization"
	authKey          ctxKey
)

// UserAuthorizationUnaryInterceptor applies user authorization to unary gRPC calls and ensures
// the user sending the query has the correct permissions to perform an action.
//
// Base authorization checks ensure a query matches the users permissions.
// Ex: a read-only user should not be able to perform a type of mutation.
//
// UserAuth is a first line of defense. More granular, row-level security is applied in
// other parts of the system, such as the ownership package.
//
// Note: this interceptor does not apply to streaming gRPC calls. Streaming calls
// need to implement their own authorization checks.
func UserAuthorizationUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (resp any, err error) {
	// Read authz from context
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx, err = validateAuthorizationKey(ctx, md)
		if err != nil {
			return nil, err
		}
	} else {
		fmt.Println("no metadata found")
		// no metadata, no authz
		return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	res, err := handler(ctx, req)
	return res, err
}

func validateAuthorizationKey(ctx context.Context, md metadata.MD) (context.Context, error) {
	if vals := md.Get(authorizationKey); len(vals) > 0 {
		// put something derived into context for handlers
		ctx = context.WithValue(ctx, authKey, vals[0])
		return ctx, nil
	}
	return ctx, status.Errorf(codes.Unauthenticated, "missing authorization")
}
