package operations

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_newError(t *testing.T) {
	req := require.New(t)

	t.Run("test error wrapping", func(t *testing.T) {
		err := newError(errInvalidFormat, "test error")
		req.NotNil(err)
		req.Implements((*error)(nil), err)

		req.Equal(errInvalidFormat, err.err)
		req.True(errors.Is(err, errInvalidFormat))
	})

	t.Run("test error wrapping with context", func(t *testing.T) {
		err := newError(errInvalidFormat, "test error: %s", "context")
		req.NotNil(err)
		req.Implements((*error)(nil), err)

		req.Equal(errInvalidFormat, err.err)
		req.True(errors.Is(err, errInvalidFormat))
		req.Equal("invalid format: test error: context", err.Error())
	})
}
