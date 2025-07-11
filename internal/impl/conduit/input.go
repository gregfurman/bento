package conduit

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/warpstreamlabs/bento/public/service"
)

type conduitStreamReader struct {
	src sdk.Source

	batcher *service.Batcher
}

func (i *conduitStreamReader) Connect(ctx context.Context) error {
	return i.src.Open(ctx, nil)
}

func (i *conduitStreamReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	record, err := i.src.Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	msg := convertToMessage(record)
	return msg, func(ctx context.Context, err error) error {
		return i.src.Ack(ctx, record.Position)
	}, nil
}

func (i *conduitStreamReader) Close(ctx context.Context) error {
	return i.src.Teardown(ctx)
}
