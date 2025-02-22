package bloblang

import (
	"context"
	"time"

	ibloblang "github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/component/cache"
)

func registerCacheBloblangPlugins(env *ibloblang.Environment) {
	// Register functions for CRUD operations against Cache
	env.RegisterFunction(cacheGetFunctionSpec(), makeKeyOpConstructor(cacheGetFunctionSpec(), cache.V1.Get))
	env.RegisterFunction(cacheDeleteFunctionSpec(), makeKeyOpConstructor(cacheDeleteFunctionSpec(), cacheDelete))
	env.RegisterFunction(cacheSetFunctionSpec(), makeValueOpConstructor(cacheSetFunctionSpec(), cache.V1.Set))
	env.RegisterFunction(cacheAddFunctionSpec(), makeValueOpConstructor(cacheAddFunctionSpec(), cache.V1.Add))
}

//------------------------------------------------------------------------------

// cacheDelete adapts a cache delete operation to fit the cacheKeyOperation signature
func cacheDelete(v cache.V1, ctx context.Context, key string) ([]byte, error) {
	if err := v.Delete(ctx, key); err != nil {
		return nil, err
	}
	return nil, nil
}

//------------------------------------------------------------------------------

// cacheValueOperation represents an operation that takes a value (i.e `set`, `add`)
type cacheValueOperation func(cache.V1, context.Context, string, []byte, *time.Duration) error

// cacheKeyOperation represents an operation that only needs a key (i.e `delete`, `get`)
type cacheKeyOperation func(cache.V1, context.Context, string) ([]byte, error)

// makeValueOpConstructor creates a constructor for cache operations that require a value
func makeValueOpConstructor(spec query.FunctionSpec, op cacheValueOperation) func(*query.ParsedParams) (query.Function, error) {
	return func(args *query.ParsedParams) (query.Function, error) {
		resource, err := args.FieldString("resource")
		if err != nil {
			return nil, err
		}

		key, err := args.FieldString("key")
		if err != nil {
			return nil, err
		}

		val, err := args.FieldString("value")
		if err != nil {
			return nil, err
		}

		return query.ClosureFunction("function "+spec.Name,
			func(fnCtx query.FunctionContext) (any, error) {
				mgr, err := getNewManagement()
				if err != nil {
					return nil, err
				}

				var cerr error
				ctx := context.Background()

				mgr.AccessCache(ctx, resource, func(v cache.V1) {
					cerr = op(v, ctx, key, []byte(val), nil)
				})

				if cerr != nil {
					return nil, cerr
				}

				return fnCtx.MsgBatch.Get(fnCtx.Index).AsBytes(), nil
			},
			nil,
		), nil
	}
}

// makeKeyOpConstructor creates a constructor for cache operations that only need a key
func makeKeyOpConstructor(spec query.FunctionSpec, op cacheKeyOperation) func(*query.ParsedParams) (query.Function, error) {
	return func(args *query.ParsedParams) (query.Function, error) {
		resource, err := args.FieldString("resource")
		if err != nil {
			return nil, err
		}

		key, err := args.FieldString("key")
		if err != nil {
			return nil, err
		}

		return query.ClosureFunction("function "+spec.Name,
			func(fnCtx query.FunctionContext) (any, error) {
				mgr, err := getNewManagement()
				if err != nil {
					return nil, err
				}

				var (
					output []byte
					cerr   error
				)
				ctx := context.Background()

				mgr.AccessCache(ctx, resource, func(v cache.V1) {
					output, cerr = op(v, ctx, key)
				})

				if cerr != nil {
					return nil, cerr
				}
				if output == nil {
					return nil, nil
				}
				return fnCtx.MsgBatch.Get(fnCtx.Index).AsBytes(), nil
			},
			nil,
		), nil
	}
}
