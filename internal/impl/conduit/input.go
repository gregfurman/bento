package conduit

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit/pkg/conduit"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterInput(
		"conduit", connectorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newConduitInput(conf)
		})
	if err != nil {
		panic(err)
	}
}

type conduitInput struct {
	src     sdk.Source
	batcher *service.Batcher
}

func newConduitInput(parsedConf *service.ParsedConfig) (service.Input, error) {
	conf, err := parseConfig(parsedConf)
	if err != nil {
		return nil, err
	}

	availablePlugins := conduit.DefaultConfig().ConnectorPlugins
	plugin, exists := availablePlugins[conf.plugin]
	if !exists {
		return nil, fmt.Errorf("plugin %s not found", conf.plugin)
	}

	src := plugin.NewSource()
	err = sdk.Util.ParseConfig(context.Background(), conf.settings, src.Config(), plugin.NewSpecification().SourceParams)
	if err != nil {
		return nil, fmt.Errorf("failed to configure source: %w", err)
	}

	return &conduitInput{
		src: src,
	}, nil
}

func (i *conduitInput) Connect(ctx context.Context) error {
	return i.src.Open(ctx, nil)
}

func (i *conduitInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	record, err := i.src.Read(ctx)
	if err != nil {
		return nil, nil, err
	}

	msg := recordToMessage(record)
	return msg, func(ctx context.Context, err error) error {
		return i.src.Ack(ctx, record.Position)
	}, nil
}

func (i *conduitInput) Close(ctx context.Context) error {
	return i.src.Teardown(ctx)
}
