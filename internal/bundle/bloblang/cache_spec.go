package bloblang

import "github.com/warpstreamlabs/bento/internal/bloblang/query"

// TODO: Should we rather be implementing a cache("name").get("key") approach where cache() returns an object that can be operated on?

func cacheGetFunctionSpec() query.FunctionSpec {
	return query.NewFunctionSpec(query.FunctionCategoryEnvironment, "cache_get", "Used to retrieve a cached value from a cache `resource`.").
		Experimental().
		MarkImpure().
		Param(query.ParamString("resource", "The name of the `cache` resource to target.")).
		Param(query.ParamString("key", "The key of the value to retrieve from the `cache` resource."))
}

func cacheDeleteFunctionSpec() query.FunctionSpec {
	return query.NewFunctionSpec(query.FunctionCategoryEnvironment, "cache_add", "Set a key in the `cache` resource to a value. If the key already exists the action fails with a 'key already exists' error.").
		Experimental().
		MarkImpure().
		Param(query.ParamString("resource", "The name of the `cache` resource to target.")).
		Param(query.ParamString("key", "A key to use with the `cache`."))
}

func cacheSetFunctionSpec() query.FunctionSpec {
	return query.NewFunctionSpec(query.FunctionCategoryEnvironment, "cache_set", "Set a key in the `cache` resource to a value. If the key already exists the contents are overridden.").
		Experimental().
		MarkImpure().
		Param(query.ParamString("resource", "The name of the `cache` resource to target.")).
		Param(query.ParamString("key", "A key to use with the `cache`.")).
		Param(query.ParamString("value", "A value to use with the `cache`."))
}

func cacheAddFunctionSpec() query.FunctionSpec {
	return query.NewFunctionSpec(query.FunctionCategoryEnvironment, "cache_add", "Set a key in the `cache` resource to a value. If the key already exists the action fails with a 'key already exists' error.").
		Experimental().
		MarkImpure().
		Param(query.ParamString("resource", "The name of the `cache` resource to target.")).
		Param(query.ParamString("key", "A key to use with the `cache`.")).
		Param(query.ParamString("value", "A value to use with the `cache`."))
}
