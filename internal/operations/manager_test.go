package operations

import (
	"github.com/litetable/litetable-db/internal/cdc_emitter"
	"github.com/litetable/litetable-db/internal/shard_storage/wal"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNew(t *testing.T) {
	t.Parallel()
	t.Run("empty config", func(t *testing.T) {
		cfg := &Config{}
		got, err := New(cfg)
		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{
			WAL: &wal.Manager{},
			CDC: &cdc_emitter.Manager{},
		}
		got, err := New(cfg)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Equal(t, int64(3600), got.defaultTTL)
	})
}
