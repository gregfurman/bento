package pure

import (
	"context"

	"github.com/warpstreamlabs/bento/internal/component/interop"
	"github.com/warpstreamlabs/bento/internal/log"
	"github.com/warpstreamlabs/bento/internal/message"
	"github.com/warpstreamlabs/bento/internal/transaction"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterBatchProcessor("sync_response", service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary("Adds the payload in its current state as a synchronous response to the input source, where it is dealt with according to that specific input type.").
		Description(`
For most inputs this mechanism is ignored entirely, in which case the sync response is dropped without penalty. It is therefore safe to use this processor even when combining input types that might not have support for sync responses. An example of an input able to utilize this is the `+"`http_server`"+`.

For more information please read xref:guides:sync_responses.adoc[synchronous responses].`).
		Field(service.NewObjectField("").Default(map[string]any{})),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			p := &syncResponseProc{log: interop.UnwrapManagement(mgr).Logger()}
			return interop.NewUnwrapInternalBatchProcessor(p), nil
		})
	if err != nil {
		panic(err)
	}
}

type syncResponseProc struct {
	log log.Modular
}

func (s *syncResponseProc) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	if err := transaction.SetAsResponse(msg); err != nil {
		s.log.Debug("Failed to store message as a sync response: %v\n", err)
	}
	return []message.Batch{msg}, nil
}

func (s *syncResponseProc) Close(context.Context) error {
	return nil
}
