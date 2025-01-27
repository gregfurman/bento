package strict

import (
	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/input"
	iprocessors "github.com/warpstreamlabs/bento/internal/component/input/processors"
	"github.com/warpstreamlabs/bento/internal/component/output"
	oprocessors "github.com/warpstreamlabs/bento/internal/component/output/processors"
	"github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/pipeline"
	"github.com/warpstreamlabs/bento/internal/pipeline/constructor"
)

// StrictBundle modifies a provided bundle environment so that all procesors
// will fail an entire batch if any any message-level error is encountered. These
// failed batches are nacked and/or reprocessed depending on your input.
func StrictBundle(b *bundle.Environment) *bundle.Environment {
	strictEnv := b.Clone()

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

//------------------------------------------------------------------------------

// NewRetryFeedbackPipelineCtor wraps a processing pipeline with a FeedbackProcessor, where failed transactions will be
// re-routed back into a Bento pipeline (and therefore re-processed).
func NewRetryFeedbackPipelineCtor() func(conf pipeline.Config, mgr bundle.NewManagement) (processor.Pipeline, error) {
	return func(conf pipeline.Config, mgr bundle.NewManagement) (processor.Pipeline, error) {
		pipe, err := constructor.New(conf, mgr)
		if err != nil {
			return nil, err
		}
		return newFeedbackProcessor(pipe, mgr), nil
	}
}

// RetryBundle wraps input.processors and output.processors pipeline constructors with FeedbackProcessors for re-routing failed transactions
// back into pipeline for retrying.
func RetryBundle(b *bundle.Environment) *bundle.Environment {
	retryEnv := b.Clone()

	for _, spec := range b.InputDocs() {
		_ = retryEnv.InputAdd(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
			pcf := iprocessors.AppendFromConfig(conf, nm)
			conf.Processors = nil

			// Wrap constructed pipeline with feedback processor
			for i, ctor := range pcf {
				pcf[i] = func() (processor.Pipeline, error) {
					pipe, err := ctor()
					if err != nil {
						return nil, err
					}
					return newFeedbackProcessor(pipe, nm), nil
				}
			}

			i, err := b.InputInit(conf, nm)
			if err != nil {
				return nil, err
			}

			wi, err := input.WrapWithPipelines(i, pcf...)
			if err != nil {
				return nil, err
			}

			return wi, nil

		}, spec)
	}

	for _, spec := range b.OutputDocs() {
		_ = retryEnv.OutputAdd(func(conf output.Config, nm bundle.NewManagement, pcf ...processor.PipelineConstructorFunc) (output.Streamed, error) {
			pcf = oprocessors.AppendFromConfig(conf, nm, pcf...)
			conf.Processors = nil

			for i, ctor := range pcf {
				pcf[i] = func() (processor.Pipeline, error) {
					pipe, err := ctor()
					if err != nil {
						return nil, err
					}
					return newFeedbackProcessor(pipe, nm), nil
				}
			}

			i, err := b.OutputInit(conf, nm)
			if err != nil {
				return nil, err
			}

			wi, err := output.WrapWithPipelines(i, pcf...)
			if err != nil {
				return nil, err
			}

			return wi, nil
		}, spec)
	}

	return retryEnv
}

//------------------------------------------------------------------------------

// NewRetryPipelineConfigCtor wraps a slice of Processor configs with a processor.retry plugin.
func NewRetryPipelineConfigCtor() func(pipeline.Config, bundle.NewManagement) (processor.Pipeline, error) {
	return func(conf pipeline.Config, mgr bundle.NewManagement) (processor.Pipeline, error) {
		conf.Processors = wrapProcessorsConfigWithRetry(conf.Processors)
		return constructor.New(conf, mgr)
	}
}

// RetryConfigBundle wraps an input.processors and output.processors config with a processor.retry plugin.
func RetryConfigBundle(b *bundle.Environment) *bundle.Environment {
	retryEnv := b.Clone()

	for _, spec := range b.InputDocs() {
		_ = retryEnv.InputAdd(iprocessors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
			conf.Processors = wrapProcessorsConfigWithRetry(conf.Processors)
			return b.InputInit(conf, nm)

		}), spec)
	}

	for _, spec := range b.OutputDocs() {
		_ = retryEnv.OutputAdd(oprocessors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
			conf.Processors = wrapProcessorsConfigWithRetry(conf.Processors)
			return b.OutputInit(conf, nm)
		}), spec)
	}

	return retryEnv
}

func wrapProcessorsConfigWithRetry(procs []processor.Config) []processor.Config {
	if len(procs) == 0 {
		return nil
	}

	processorSlice := make([]any, len(procs))
	for i, p := range procs {
		processorSlice[i] = p
	}

	wrappedRetryPipeConf := processor.Config{
		Type: "retry",
		Plugin: map[string]any{
			"processors": processorSlice},
	}

	return []processor.Config{wrappedRetryPipeConf}
}
