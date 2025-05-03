package engine

import (
	"github.com/litetable/litetable-db/internal/operations"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNew(t *testing.T) {
	t.Parallel()

	req := require.New(t)
	t.Run("invalid config", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			OperationManager: nil,
			MaxBufferSize:    4096,
		}
		engine, err := New(cfg)
		req.Error(err)
		req.Nil(engine)
	})

	t.Run("valid config", func(t *testing.T) {
		t.Parallel()
		cfg := &Config{
			OperationManager: &operations.Manager{},
			MaxBufferSize:    4096,
		}
		engine, err := New(cfg)
		req.NoError(err)
		req.NotNil(engine)
	})
}

func TestEngine(t *testing.T) {
	t.Parallel()
	req := require.New(t)
	t.Run("Start()", func(t *testing.T) {
		t.Parallel()
		e := &Engine{}
		err := e.Start()
		req.NoError(err)
	})

	t.Run("Stop()", func(t *testing.T) {
		t.Parallel()
		e := &Engine{}
		err := e.Stop()
		req.NoError(err)
	})

	t.Run("Name()", func(t *testing.T) {
		t.Parallel()
		e := &Engine{}
		name := e.Name()
		req.Equal("LiteTable Engine", name)
	})
}
