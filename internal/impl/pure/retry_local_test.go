package pure

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// func TestLocalRetryConfErrors(t *testing.T) {
// 	conf, err := localRetryConfig().ParseYAML(`max_retries: -1`, nil)
// 	require.NoError(t, err)

// 	_, err = newLocalRetryFromConfig(conf)
// 	require.Error(t, err)

// 	conf, err = localRetryConfig().ParseYAML(`backoff.initial_interval: -1`, nil)
// 	require.NoError(t, err)

// 	_, err = newLocalRetryFromConfig(conf)
// 	require.Error(t, err)

// 	conf, err = localRetryConfig().ParseYAML(`backoff.max_interval: -1`, nil)
// 	require.NoError(t, err)

// 	_, err = newLocalRetryFromConfig(conf)
// 	require.Error(t, err)

// 	conf, err = localRetryConfig().ParseYAML(`backoff.max_elapsed_time: -1`, nil)
// 	require.NoError(t, err)

// 	_, err = newLocalRetryFromConfig(conf)
// 	require.Error(t, err)
// }

func TestLocalRetryBasic(t *testing.T) {
	conf, err := localRetryConfig().ParseYAML(`
max_retries: 0
backoff:
  initial_interval: 100ms
  max_interval: 1s
  max_elapsed_time: 5s
  randomization_factor: 0
`, nil)
	require.NoError(t, err)

	retrier, err := newLocalRetryFromConfig(conf)
	require.NoError(t, err)

	curr := 100.0
	ctx := context.Background()

	err = retrier.TriggerBackoff(context.Background())
	require.NoError(t, err)

	start := time.Now()
	for i := 0; i < 10; i++ {
		period, err := retrier.Access(ctx)
		require.NoError(t, err)

		// Note: backoff seems to reset at 4s instead of after 5s
		// Backoff is exhausted, so will return 0
		if time.Since(start) >= 4*time.Second {
			assert.Zero(t, period)
			continue
		}

		assert.NotZero(t, period)
		assert.Equal(t, time.Duration(curr*float64(time.Millisecond)), period)

		time.Sleep(period)

		next := period.Seconds() * 1000 * 1.5
		curr = min(next, 1000)
	}

	assert.Equal(t, 8, retrier.currentRetries)

	err = retrier.Reset(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, retrier.currentRetries)
}

func TestLocalRetryEnableDisable(t *testing.T) {
	conf, err := localRetryConfig().ParseYAML(`
max_retries: 0
backoff:
  initial_interval: 1s
  randomization_factor: 0
`, nil)
	require.NoError(t, err)

	retrier, err := newLocalRetryFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	period, err := retrier.Access(ctx)
	require.NoError(t, err)
	assert.Zero(t, period)

	err = retrier.TriggerBackoff(context.Background())
	require.NoError(t, err)
	assert.True(t, retrier.enabled)

	period, err = retrier.Access(ctx)
	require.NoError(t, err)
	assert.Equal(t, time.Second, period)

	err = retrier.Reset(ctx)
	require.NoError(t, err)
	assert.False(t, retrier.enabled)

	period, err = retrier.Access(ctx)
	require.NoError(t, err)
	assert.Zero(t, period)
}

func TestLocalRetryConcurrentAccess(t *testing.T) {
	conf, err := localRetryConfig().ParseYAML(`
max_retries: 0
backoff:
  initial_interval: 100ms
  max_interval: 1s
  max_elapsed_time: 5s
  randomization_factor: 0
`, nil)
	require.NoError(t, err)

	retrier, err := newLocalRetryFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	workerCount := 3
	startCh := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(workerCount)

	err = retrier.TriggerBackoff(context.Background())
	require.NoError(t, err)

	start := time.Now()
	for w := 0; w < workerCount; w++ {
		go func(worker int) {
			<-startCh
			workerStart := time.Now()
			for {
				period, err := retrier.Access(ctx)
				require.NoError(t, err)

				if period == 0 {
					defer wg.Done()
					require.GreaterOrEqual(t, time.Since(workerStart), 4*time.Second)
					return
				}

				require.LessOrEqual(t, period, 5*time.Second)
			}

		}(w)
	}
	// Signal to start all goroutines concurrently
	close(startCh)

	wg.Wait()
	require.GreaterOrEqual(t, time.Since(start), 4*time.Second)
}

func TestLocalRetryConcurrentAccessDifferentStart(t *testing.T) {
	conf, err := localRetryConfig().ParseYAML(`
max_retries: 0
backoff:
  initial_interval: 100ms
  max_interval: 1s
  max_elapsed_time: 5s
  randomization_factor: 0
`, nil)
	require.NoError(t, err)

	retrier, err := newLocalRetryFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	workerCount := 3

	var wg sync.WaitGroup
	wg.Add(workerCount)

	err = retrier.TriggerBackoff(context.Background())
	require.NoError(t, err)

	start := time.Now()
	for w := 0; w < workerCount; w++ {
		go func(worker int) {
			jitter := time.Duration(rand.Float32() * float32(time.Second))
			time.Sleep(jitter)
			for {
				period, err := retrier.Access(ctx)
				require.NoError(t, err)

				if period == 0 {
					defer wg.Done()
					return
				}

				require.LessOrEqual(t, period, 5*time.Second)
			}

		}(w)
	}
	wg.Wait()
	require.GreaterOrEqual(t, time.Since(start), 4*time.Second)
	require.Equal(t, 8, retrier.currentRetries)
}
