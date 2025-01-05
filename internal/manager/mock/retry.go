package mock

import (
	"context"
	"time"
)

// Retry provides a mock retry implementation around a closure.
type Retry func(context.Context) (time.Duration, error)

// Access the retry resource.
func (r Retry) Access(ctx context.Context) (time.Duration, error) {
	return r(ctx)
}

func (r Retry) TriggerBackoff(ctx context.Context) error {
	return nil
}

// Close does nothing.
func (r Retry) Close(ctx context.Context) error {
	return nil
}

// Reset does nothing.
func (r Retry) Reset(ctx context.Context) error {
	return nil
}
