package pure

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/warpstreamlabs/bento/internal/retries"
	"github.com/warpstreamlabs/bento/public/service"
)

func localRetryConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Summary(`A shared resource for calculating exponential backoff between retries.`).
		Fields(retries.CommonRetryBackOffFields(0, "1s", "5s", "30s")...)
	return spec
}

func init() {
	err := service.RegisterRetry(
		"local", localRetryConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Retry, error) {
			return newLocalRetryFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newLocalRetryFromConfig(conf *service.ParsedConfig) (*localRetry, error) {
	boffCtor, err := retries.CommonRetryBackOffCtorFromParsed(conf)
	if err != nil {
		return nil, err
	}

	boff := boffCtor()
	return newLocalRetry(boff)
}

//------------------------------------------------------------------------------

type localRetry struct {
	enabled bool
	backoff backoff.BackOff

	currentRetries  int
	backoffDuration time.Duration
	lastRetry       time.Time

	mut sync.RWMutex
}

func newLocalRetry(boff backoff.BackOff) (*localRetry, error) {

	return &localRetry{
		backoff:   boff,
		lastRetry: time.Now(),
		enabled:   false,
	}, nil
}

func (r *localRetry) Access(_ context.Context) (time.Duration, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	if !r.enabled {
		return 0, nil
	}

	if remaining := r.backoffDuration - time.Since(r.lastRetry); remaining > 0 {
		// Should this be jittered since multiple components could be waiting and flush at the same time?
		return remaining, nil
	}

	duration := r.backoff.NextBackOff()
	if duration == backoff.Stop {
		r.enabled = false
		return 0, nil
	}

	r.lastRetry = time.Now()
	r.currentRetries++
	r.backoffDuration = duration

	return duration, nil
}

func (r *localRetry) TriggerBackoff(ctx context.Context) error {
	r.mut.Lock()
	defer r.mut.Unlock()

	r.enabled = true
	r.backoff.Reset()
	return nil
}

func (r *localRetry) Reset(_ context.Context) error {
	r.mut.Lock()
	defer r.mut.Unlock()

	r.enabled = false
	r.backoff.Reset()
	r.currentRetries = 0
	r.backoffDuration = 0

	return nil
}

func (r *localRetry) Close(_ context.Context) error {
	r.Reset(context.TODO())
	return nil
}
