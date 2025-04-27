package protocol

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_newError(t *testing.T) {
	req := require.New(t)

	t.Run("test error wrapping", func(t *testing.T) {
		err := newError(ErrInvalidFormat, "test error")
		req.NotNil(err)
		req.Implements((*error)(nil), err)

		req.Equal(ErrInvalidFormat, err.Err)
		req.True(errors.Is(err, ErrInvalidFormat))
	})

	t.Run("test error wrapping with context", func(t *testing.T) {
		err := newError(ErrInvalidFormat, "test error: %s", "context")
		req.NotNil(err)
		req.Implements((*error)(nil), err)

		req.Equal(ErrInvalidFormat, err.Err)
		req.True(errors.Is(err, ErrInvalidFormat))
		req.Equal("invalid format: test error: context", err.Error())
	})
}
