package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/public/service"
)

func redisRatelimitConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Summary(`A rate limit implementation using Redis. It works by using a simple token bucket algorithm to limit the number of requests to a given count within a given time period. The rate limit is shared across all instances of Bento that use the same Redis instance, which must all have a consistent count and interval.`).
		Version("1.0.0")

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	spec.Field(
		service.NewIntField("count").
			Description("The maximum number of requests to allow for a given period of time. If `0` disables count based rate-limiting.").
			Default(1000).LintRule(`root = if this < 0 { [ "count cannot be less than zero" ] }`)).
		Field(service.NewIntField("byte_size").
			Description("The maximum number of bytes to allow for a given period of time. If `0` disables size based rate-limiting.").
			Default(0).LintRule(`root = if this < 0 { [ "byte_size cannot be less than zero" ] }`)).
		Field(service.NewDurationField("interval").
			Description("The time window to limit requests by.").
			Default("1s")).
		Field(service.NewStringField("key").
			Description("The key to use for the rate limit."))

	return spec
}

func init() {
	err := service.RegisterRateLimit(
		"redis", redisRatelimitConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			return newRedisRatelimitFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type redisRatelimit struct {
	size     int
	byteSize int
	key      string
	period   time.Duration

	client redis.UniversalClient

	accessScript *redis.Script
	addScript    *redis.Script
}

func newRedisRatelimitFromConfig(conf *service.ParsedConfig) (*redisRatelimit, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	count, err := conf.FieldInt("count")
	if err != nil {
		return nil, err
	}

	byteSize, err := conf.FieldInt("byte_size")
	if err != nil {
		return nil, err
	}

	interval, err := conf.FieldDuration("interval")
	if err != nil {
		return nil, err
	}

	key, err := conf.FieldString("key")
	if err != nil {
		return nil, err
	}

	if byteSize < 0 || count < 0 {
		return nil, errors.New("neither byte size nor count can be negative")
	}

	if byteSize == 0 && count == 0 {
		return nil, errors.New("either count or byte size must be larger than zero")
	}

	return &redisRatelimit{
		size:     count,
		byteSize: byteSize,
		period:   interval,
		client:   client,
		key:      key,
		accessScript: redis.NewScript(`
local count_limit = tonumber(ARGV[1])
local byte_limit = tonumber(ARGV[2])
local expire_ms = tonumber(ARGV[3])

if redis.call("TTL", KEYS[1]) == -1 then
	redis.call("PEXPIRE", KEYS[1], expire_ms)
end

if count_limit > 0 then
	local current = redis.call("HINCRBY", KEYS[1], "count", 1)

    if current > count_limit then
        return redis.call("PTTL", KEYS[1])
    end
end

if byte_limit > 0 then
	local bytes = redis.call("HGET", KEYS[1], "bytes")
	if tonumber(bytes) > byte_limit then
    	return redis.call("PTTL", KEYS[1])
	end
end

return 0
`),
		addScript: redis.NewScript(`
local byte_limit = tonumber(ARGV[2])
local expire_ms = tonumber(ARGV[3])
local bytes_to_add = tonumber(ARGV[1])

local current = redis.call("HINCRBY", KEYS[1], "bytes", bytes_to_add)

if redis.call("TTL", KEYS[1]) == -1 then
	redis.call("PEXPIRE", KEYS[1], expire_ms)
end

if current > byte_limit then
	return redis.call("PTTL", KEYS[1])
end

return 0
		`),
	}, nil
}

//------------------------------------------------------------------------------

func (r *redisRatelimit) Access(ctx context.Context) (time.Duration, error) {
	result := r.accessScript.Run(ctx, r.client, []string{r.key}, r.size, r.byteSize, int(r.period.Milliseconds()))

	if result.Err() != nil {
		return 0, fmt.Errorf("accessing redis rate limit: %w", result.Err())
	}

	if result.Val() == 0 {
		return 0, nil
	}

	return time.Duration((result.Val().(int64)) * int64(time.Millisecond)), nil
}

func (r *redisRatelimit) Add(ctx context.Context, parts ...*message.Part) bool {
	if r.byteSize <= 0 || len(parts) == 0 {
		return false
	}

	var totalBytes int
	for _, part := range parts {
		if part != nil {
			totalBytes += len(part.AsBytes())
		}
	}

	result := r.addScript.Run(ctx, r.client,
		[]string{r.key},              // KEYS[1]: key for hashset
		totalBytes,                   // ARGV[1]: bytes to add
		r.byteSize,                   // ARGV[2]: byte limit
		int(r.period.Milliseconds())) // ARGV[3]: expiry in ms

	if result.Err() != nil {
		return false
	}

	exceeded, ok := result.Val().(int64)
	return ok && exceeded > 0
}

func (r *redisRatelimit) Close(ctx context.Context) error {
	return nil
}
