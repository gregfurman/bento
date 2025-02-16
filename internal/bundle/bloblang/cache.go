package bloblang

import (
	"context"

	ibloblang "github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/query"
	"github.com/warpstreamlabs/bento/internal/component/cache"
)

func registerCacheBloblangPlugins(env *ibloblang.Environment) {
	// Register functions for CRUD operations against Cache
	env.RegisterFunction(cacheGetFunctionSpec(), makeKeyOpConstructor(cacheGetFunctionSpec(), cacheGet))
	env.RegisterFunction(cacheDeleteFunctionSpec(), makeKeyOpConstructor(cacheDeleteFunctionSpec(), cacheDelete))
	env.RegisterFunction(cacheSetFunctionSpec(), makeValueOpConstructor(cacheSetFunctionSpec(), cacheSet))
	env.RegisterFunction(cacheAddFunctionSpec(), makeValueOpConstructor(cacheAddFunctionSpec(), cacheAdd))
}

//------------------------------------------------------------------------------

// cacheValueOperation represents an operation that takes a value (i.e `set`, `add`)
type cacheValueOperation func(context.Context, cache.V1, string, []byte) error

// cacheKeyOperation represents an operation that only needs a key (i.e `delete`, `get`)
type cacheKeyOperation func(context.Context, cache.V1, string) ([]byte, error)

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

		value, err := args.FieldString("value")
		if err != nil {
			return nil, err
		}

		return query.ClosureFunction("function "+spec.Name,
			func(_ query.FunctionContext) (any, error) {
				mgr, err := getNewManagement()
				if err != nil {
					return nil, err
				}

				var cerr error
				ctx := context.Background()

				mgr.AccessCache(ctx, resource, func(v cache.V1) {
					cerr = op(ctx, v, key, []byte(value))
				})

				if cerr != nil {
					return nil, cerr
				}
				return value, nil
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
			func(_ query.FunctionContext) (any, error) {
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
					output, cerr = op(ctx, v, key)
				})

				if cerr != nil {
					return nil, cerr
				}
				if output == nil {
					return nil, nil
				}
				return string(output), nil
			},
			nil,
		), nil
	}
}

//------------------------------------------------------------------------------

// Cache operations

func cacheGet(ctx context.Context, v cache.V1, key string) ([]byte, error) {
	return v.Get(ctx, key)
}

func cacheDelete(ctx context.Context, v cache.V1, key string) ([]byte, error) {
	if err := v.Delete(ctx, key); err != nil {
		return nil, err
	}
	return nil, nil
}

func cacheSet(ctx context.Context, v cache.V1, key string, value []byte) error {
	return v.Set(ctx, key, value, nil)
}

func cacheAdd(ctx context.Context, v cache.V1, key string, value []byte) error {
	return v.Add(ctx, key, value, nil)
}
