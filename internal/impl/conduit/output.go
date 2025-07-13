package conduit

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit/pkg/conduit"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterOutput(
		"conduit", connectorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			output, err := newConduitOutput(conf)

			// n, err := conf.FieldInt("max_in_flight")
			// if err != nil {
			// 	return nil, 0, err
			// }

			return output, 1, err
		})
	if err != nil {
		panic(err)
	}
}

type conduitOutput struct {
	dest      sdk.Destination
	operation opencdc.Operation
}

func newConduitOutput(parsedConf *service.ParsedConfig) (service.Output, error) {
	conf, err := parseConfig(parsedConf)
	if err != nil {
		return nil, err
	}

	availablePlugins := conduit.DefaultConfig().ConnectorPlugins
	plugin, exists := availablePlugins[conf.plugin]
	if !exists {
		return nil, fmt.Errorf("plugin %s not found", conf.plugin)
	}

	dest := plugin.NewDestination()
	err = sdk.Util.ParseConfig(context.Background(), conf.settings, dest.Config(), plugin.NewSpecification().DestinationParams)
	if err != nil {
		return nil, fmt.Errorf("failed to configure destination: %w", err)
	}

	return &conduitOutput{
		dest:      dest,
		operation: opencdc.OperationCreate,
	}, nil
}

func (w *conduitOutput) Connect(ctx context.Context) error {
	return w.dest.Open(ctx)
}

func (w *conduitOutput) Write(ctx context.Context, msg *service.Message) error {
	record, err := messageToRecord(msg)
	if err != nil {
		return err
	}

	_, err = w.dest.Write(ctx, []opencdc.Record{record})
	return err
}

func (w *conduitOutput) Close(ctx context.Context) error {
	return w.dest.Teardown(ctx)
}
