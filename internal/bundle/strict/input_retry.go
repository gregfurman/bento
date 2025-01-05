package strict

import (
	"context"
	"time"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/component/retry"
	"github.com/warpstreamlabs/bento/internal/message"
)

type iwrapped input.Streamed

type retryInput struct {
	iwrapped

	ctx           context.Context
	retryResource string
	mg            bundle.NewManagement

	tChan   chan message.Transaction
	shutSig *shutdown.Signaller
}

func retryableInput(resource string, i input.Streamed, mg bundle.NewManagement) input.Streamed {
	t := &retryInput{
		iwrapped:      i,
		shutSig:       shutdown.NewSignaller(),
		tChan:         make(chan message.Transaction),
		mg:            mg,
		retryResource: resource,
	}
	go t.loop()
	return t
}

func (t *retryInput) UnwrapInput() input.Streamed {
	return t.iwrapped
}

func (t *retryInput) loop() {
	defer close(t.tChan)
	readChan := t.iwrapped.TransactionChan()
	for {
		var tran message.Transaction
		var open bool
		select {
		case tran, open = <-readChan:
			if !open {
				return
			}
		case <-t.shutSig.HardStopChan():
			return
		}
		var backoffDuration time.Duration
		_ = t.mg.AccessRetry(t.ctx, t.retryResource, func(v retry.V1) {
			backoffDuration, _ = v.Access(t.ctx)
		})

		if backoffDuration > 0 {
			select {
			case <-time.After(backoffDuration):
			case <-t.shutSig.HardStopChan():
				return
			}
		}

		select {
		case t.tChan <- tran:
		case <-t.shutSig.HardStopChan():
			// Stop flushing if we fully timed out
			return
		}
	}
}

func (r *retryInput) TransactionChan() <-chan message.Transaction {
	return r.tChan
}
