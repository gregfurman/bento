package service

import (
	"context"
	"time"

	"github.com/warpstreamlabs/bento/internal/component/metrics"
	"github.com/warpstreamlabs/bento/internal/component/retry"
)

// Retry is an interface implemented by Bento retry resources.
type Retry interface {
	Access(context.Context) (time.Duration, error)

	TriggerBackoff(ctx context.Context) error

	Reset(context.Context) error

	Closer
}

//------------------------------------------------------------------------------

func newAirGapRetry(c Retry, stats metrics.Type) retry.V1 {
	return c
}

//------------------------------------------------------------------------------

// Implements types.MessageAwareRateLimit.
type airGapRetry struct {
	r Retry
}

func (a *airGapRetry) Access(ctx context.Context) (time.Duration, error) {
	return a.r.Access(ctx)
}

func (a *airGapRetry) Close(ctx context.Context) error {
	return a.r.Close(ctx)
}

func (a *airGapRetry) TriggerBackoff(ctx context.Context) error {
	return a.r.TriggerBackoff(ctx)
}

func (a *airGapRetry) Reset(ctx context.Context) error {
	return a.r.Reset(ctx)
}

//------------------------------------------------------------------------------

// // Implements RateLimit around a types.RateLimit.
// type reverseAirGapRateLimit struct {
// 	r ratelimit.V1
// }

// func newReverseAirGapRateLimit(r ratelimit.V1) *reverseAirGapRateLimit {
// 	return &reverseAirGapRateLimit{r}
// }

// func (a *reverseAirGapRateLimit) Access(ctx context.Context) (time.Duration, error) {
// 	return a.r.Access(ctx)
// }

// func (a *reverseAirGapRateLimit) Close(ctx context.Context) error {
// 	return a.r.Close(ctx)
// }
