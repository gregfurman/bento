package retries

import (
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	crboFieldMaxRetries          = "max_retries"
	crboFieldBackOff             = "backoff"
	crboFieldInitInterval        = "initial_interval"
	crboFieldMaxInterval         = "max_interval"
	crboFieldMaxElapsedTime      = "max_elapsed_time"
	crboFieldRandomizationFactor = "randomization_factor"
)

func CommonRetryBackOffFields(
	defaultMaxRetries int,
	defaultInitInterval string,
	defaultMaxInterval string,
	defaultMaxElapsed string,
) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewIntField(crboFieldMaxRetries).
			Description("The maximum number of retries before giving up on the request. If set to zero there is no discrete limit.").
			Default(defaultMaxRetries).
			Advanced(),
		service.NewObjectField(crboFieldBackOff,
			service.NewDurationField(crboFieldInitInterval).
				Description("The initial period to wait between retry attempts.").
				Default(defaultInitInterval),
			service.NewDurationField(crboFieldMaxInterval).
				Description("The maximum period to wait between retry attempts.").
				Default(defaultMaxInterval),
			service.NewDurationField(crboFieldMaxElapsedTime).
				Description("The maximum period to wait before retry attempts are abandoned. If zero then no limit is used.").
				Default(defaultMaxElapsed),
			service.NewFloatField("The factor used to add random jitter to each caclulated interval where `Randomized interval = RetryInterval * (1 ± RandomizationFactor)`. If zero then no jitter is used.").
				Default(0.5).
				Advanced(),
		).
			Description("Control time intervals between retry attempts.").
			Advanced(),
	}
}

func fieldDurationOrEmptyStr(pConf *service.ParsedConfig, path ...string) (time.Duration, error) {
	if dStr, err := pConf.FieldString(path...); err == nil && dStr == "" {
		return 0, nil
	}
	return pConf.FieldDuration(path...)
}

func CommonRetryBackOffCtorFromParsed(pConf *service.ParsedConfig) (ctor func() backoff.BackOff, err error) {
	var opts []backoff.ExponentialBackOffOpts

	var maxRetries int
	if maxRetries, err = pConf.FieldInt(crboFieldMaxRetries); err != nil {
		return
	}

	var initInterval, maxInterval, maxElapsed time.Duration
	var randomFactor float64
	if pConf.Contains(crboFieldBackOff) {
		bConf := pConf.Namespace(crboFieldBackOff)
		if initInterval, err = fieldDurationOrEmptyStr(bConf, crboFieldInitInterval); err != nil {
			return
		}
		opts = append(opts, backoff.WithInitialInterval(initInterval))

		if maxInterval, err = fieldDurationOrEmptyStr(bConf, crboFieldMaxInterval); err != nil {
			return
		}

		opts = append(opts, backoff.WithMaxInterval(maxInterval))

		if maxElapsed, err = fieldDurationOrEmptyStr(bConf, crboFieldMaxElapsedTime); err != nil {
			return
		}

		opts = append(opts, backoff.WithMaxElapsedTime(maxElapsed))

		if bConf.Contains(crboFieldRandomizationFactor) {
			if randomFactor, err = bConf.FieldFloat(crboFieldRandomizationFactor); err != nil {
				return
			}
			opts = append(opts, backoff.WithRandomizationFactor(randomFactor))
		}
	}

	return func() backoff.BackOff {
		boff := backoff.NewExponentialBackOff(opts...)

		if maxRetries > 0 {
			return backoff.WithMaxRetries(boff, uint64(maxRetries))
		}
		return boff
	}, nil
}
