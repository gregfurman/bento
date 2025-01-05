package strict

import (
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/processor"
)

type option func(o *options)

func WithRetryResource(resource string) option {
	return func(o *options) {
		o.resource = resource
	}
}

type options struct {
	resource string
}

// StrictBundle modifies a provided bundle environment so that all procesors
// will fail an entire batch if any any message-level error is encountered. These
// failed batches are nacked and/or reprocessed depending on your input.
func StrictBundle(b *bundle.Environment, opts ...option) *bundle.Environment {
	strictEnv := b.Clone()

	cfg := &options{}
	for _, opt := range opts {
		opt(cfg)
	}

	for _, spec := range b.InputDocs() {
		_ = strictEnv.InputAdd(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
			i, err := b.InputInit(conf, nm)
			if err != nil {
				return nil, err
			}
			i = retryableInput(cfg.resource, i, nm)
			return i, err
		}, spec)
	}

	for _, spec := range b.ProcessorDocs() {
		_ = strictEnv.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
			proc, err := b.ProcessorInit(conf, nm)
			if err != nil {
				return nil, err
			}
			proc = wrapWithStrict(proc)
			return proc, err
		}, spec)
	}

	// TODO: Overwrite inputs for retry with backoff

	return strictEnv
}

func RetryBundle(b *bundle.Environment) *bundle.Environment {
	retryEnv := b.Clone()

	for _, spec := range b.InputDocs() {
		_ = retryEnv.InputAdd(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
			i, err := b.InputInit(conf, nm)
			if err != nil {
				return nil, err
			}
			i = retryableInput("", i, nm)
			return i, err
		}, spec)
	}

	return retryEnv
}
