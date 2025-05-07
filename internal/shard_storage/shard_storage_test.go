package shard_storage

import (
	"fmt"
	"github.com/google/uuid"
	"math"
	"math/rand"
	"testing"
)

func TestGetShardIndex(t *testing.T) {
	tests := map[string]struct {
		shardCount int
		rowKeys    []string
	}{
		"single shard returns zero index": {
			shardCount: 1,
			rowKeys:    []string{"champ:1", "champ:2", "champ:3"},
		},
		"multiple shards distribute keys": {
			shardCount: 8,
			rowKeys:    []string{"champ:1", "champ:2", "champ:3", "keyA", "keyB", "keyC", "o"},
		},
		"large number of shards": {
			shardCount: 64,
			rowKeys:    []string{"user:1", "user:2", "post:10", "post:11", "comment:5"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			manager := &Manager{
				shardCount: tc.shardCount,
			}

			// Test that indices are within bounds
			for _, key := range tc.rowKeys {
				idx := manager.getShardIndex(key)

				if idx < 0 || idx >= tc.shardCount {
					t.Errorf("Index %d out of bounds for shard count %d", idx, tc.shardCount)
				}

			}

			// getShardIndex is deterministic; we should test it against
			// the same key should always return the index based on the shard count
			for _, key := range tc.rowKeys {
				first := manager.getShardIndex(key)
				// test each key a hundred times to ensure consistency
				for i := 0; i < 100; i++ {
					nextShardIndex := manager.getShardIndex(key)
					if nextShardIndex != first {
						t.Errorf("Inconsistent shard index. Expected '%v' and got  '%v'", first,
							nextShardIndex)
						break
					}
				}
			}

		})
	}
}

// TestGetShardIndexDistribution is my try-hard attempt to understand if the distribution of
// shards is even. It is probably not perfect, but it should be a good start.
//
// What are we testing?
// - That we distribute the load evenly across shards
// - Avoid "hot spots" where too many keys end up in the same shard
// - Maintain consistent performance as data grows
func TestGetShardIndexDistribution(t *testing.T) {

	shardCount := 16
	keyCount := 1000000 // one million keys
	manager := &Manager{shardCount: shardCount}

	// Count occurrences per shard
	distribution := make([]int, shardCount)

	for i := 0; i < keyCount; i++ {
		// generate a random string between 3-20 characters
		// and use it as a key

		ran := randomString(3, 20)
		key := fmt.Sprintf("%s:%s", ran, uuid.NewString())
		idx := manager.getShardIndex(key)
		distribution[idx]++
	}

	// Calculate statistics
	mean := float64(keyCount) / float64(shardCount)
	variance := 0.0

	for _, count := range distribution {
		variance += math.Pow(float64(count)-mean, 2)
	}

	variance /= float64(shardCount)
	stdDev := math.Sqrt(variance)

	// Check if distribution is within 3 standard deviations
	maxDeviation := 3.0 * stdDev

	for i, count := range distribution {
		deviation := math.Abs(float64(count) - mean)
		if deviation > maxDeviation {
			t.Errorf("Shard %d has count %d, which deviates too much from mean %.2f (deviation: %.2f > %.2f)",
				i, count, mean, deviation, maxDeviation)
		}
	}

	// Calculate the percentage deviation (stdDev as a percentage of mean)
	deviationPercent := (stdDev / mean) * 100.0

	// Log distribution stats with percentage
	t.Logf("Distribution stats: mean=%.2f, stdDev=%.2f (%.2f%% of mean)", mean, stdDev, deviationPercent)
}

func randomString(min, max int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	length := min
	if max > min {
		length += rand.Intn(max - min + 1)
	}

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}

	return string(b)
}
