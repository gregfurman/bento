package strict

import (
	"context"

	"github.com/Jeffail/shutdown"

	"github.com/warpstreamlabs/bento/internal/bundle"
	"github.com/warpstreamlabs/bento/internal/component/input"
	"github.com/warpstreamlabs/bento/internal/message"
)

type pauseInput struct {
	iwrapped

	ctx           context.Context
	retryResource string
	nm            bundle.NewManagement

	pausedCh <-chan bool

	tChan   chan message.Transaction
	shutSig *shutdown.Signaller
}

func inputPauser(i input.Streamed) input.Streamed {
	t := &pauseInput{
		iwrapped: i,
		tChan:    make(chan message.Transaction),
	}
	go t.loop()
	return t
}

func (t *pauseInput) UnwrapInput() input.Streamed {
	return t.iwrapped
}

func (t *pauseInput) loop() {
	defer close(t.tChan)
	readChan := t.iwrapped.TransactionChan()

	paused := false
	for {
		if paused {
			var ok bool
			select {
			case paused, ok = <-t.pausedCh:
				if !ok {
					return
				}
			case <-t.shutSig.HardStopChan():
				return
			}
			continue
		}

		var tran message.Transaction
		var open bool
		select {
		case paused = <-t.pausedCh:
			continue
		case tran, open = <-readChan:
			if !open {
				return
			}
		case <-t.shutSig.HardStopChan():
			return
		}
		// var backoffDuration time.Duration
		// _ = t.nm.AccessRetry(t.ctx, t.retryResource, func(v retry.V1) {
		// 	backoffDuration, _ = v.Access(t.ctx)
		// })

		// trackedTran := transaction.NewTracked(tran.Payload, tran.Ack)
		// _ = trackedTran.Message().Iter(func(i int, p *message.Part) error {
		// 	return nil
		// })

		// if backoffDuration > 0 {
		// 	select {
		// 	case <-time.After(backoffDuration):
		// 	case <-t.shutSig.HardStopChan():
		// 		return
		// 	}
		// }

		select {
		case t.tChan <- tran:
		case <-t.shutSig.HardStopChan():
			// Stop flushing if we fully timed out
			return
		}
	}
}
