package strict

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/batch"
	iprocessor "github.com/warpstreamlabs/bento/internal/component/processor"
	"github.com/warpstreamlabs/bento/internal/message"
)

func wrapWithStrict(p iprocessor.V1) iprocessor.V1 {
	t := &strictProcessor{
		wrapped: p,
		enabled: true,
	}
	return t
}

//------------------------------------------------------------------------------

// strictProcessor fails batch processing if any message contains an error.
type strictProcessor struct {
	wrapped iprocessor.V1
	enabled bool
}

func (s *strictProcessor) ProcessBatch(ctx context.Context, b message.Batch) ([]message.Batch, error) {
	if !s.enabled {
		return s.wrapped.ProcessBatch(ctx, b)
	}

	batches, err := s.wrapped.ProcessBatch(ctx, b)
	if err != nil {
		return nil, err
	}

	// Iterate through all messages and populate a batch.Error type, calling Failed()
	// for each errored message. Otherwise, every message in the batch is treated as a failure.
	for _, msg := range batches {
		var batchErr *batch.Error
		_ = msg.Iter(func(i int, p *message.Part) error {
			mErr := p.ErrorGet()
			if mErr == nil {
				return nil
			}
			if batchErr == nil {
				batchErr = batch.NewError(msg, mErr)
			}
			batchErr.Failed(i, mErr)

			// Clear the message-level error
			p.ErrorSet(nil)
			return nil
		})
		if batchErr != nil {
			return nil, batchErr
		}
	}

	return batches, nil
}

func (s *strictProcessor) Close(ctx context.Context) error {
	return s.wrapped.Close(ctx)
}

func (s *strictProcessor) UnwrapProc() iprocessor.V1 {
	return s.wrapped
}
