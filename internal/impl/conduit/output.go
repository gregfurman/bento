package conduit

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

type conduitStreamWriter struct {
	dest sdk.Destination

	// batcher *service.Batcher

	positonExec service.MessageBatchInterpolationExecutor
	blobl       *bloblang.Executor
}

func (w *conduitStreamWriter) Connect(ctx context.Context) error {
	return w.dest.Open(ctx)
}

func (w *conduitStreamWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	w.dest.Write()
}

func (w *conduitStreamWriter) Close(ctx context.Context) error {
	return w.dest.Teardown(ctx)
}
