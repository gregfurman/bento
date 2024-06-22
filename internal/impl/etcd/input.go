package etcd

import (
	"context"

	"github.com/warpstreamlabs/bento/public/service"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func etcdWatchFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(etcdKeyField).
			Description("The key or prefix being watched.").
			Default(""),
		service.NewObjectField(etcdOperationOptions,
			service.NewBoolField(etcdWithPrefixField).
				Description("Whether to watch for events on a prefix.").
				Default(false),
			service.NewBoolField(etcdWatchWithProgressNotifyField).
				Description("Whether to send periodic progress updates every 10 minutes when there is no incoming events.").
				Default(false),
			service.NewBoolField(etcdWatchWithFilterPut).
				Description("Whether to discard PUT events from the watcher.").
				Default(false),
			service.NewBoolField(etcdWatchWithFilterDelete).
				Description("Whether to discard DELETE events from the watcher.").
				Default(false),
			service.NewBoolField(etcdWatchWithCreatedNotifyField).
				Description("Whether to send CREATED notify events to the watcher.").
				Default(false),
			service.NewStringField(etcdWatchWithRangeField).
				Description("Will cause the watcher to return a range of lexicographically sorted keys to return in the form `[key, end)` where `end` is the passed parameter.").
				Default(""),
		).Description("Collection of options to configure an etcd watcher."),
	}
}

func etcdConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("Configures an etcd event watcher.").
		Description(`Watches an etcd key or prefix for new events.`). // TODO: Add more documentation
		Fields(etcdClientFields()...).
		Fields(etcdWatchFields()...).
		Field(service.NewAutoRetryNacksToggleField())

	return spec
}

func newEtcdWatchInput(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	reader, err := newEtcdWatchInputFromConfig(conf, mgr)
	if err != nil {
		return nil, err
	}

	return service.AutoRetryNacksToggled(conf, reader)
}

func init() {
	err := service.RegisterInput("etcd", etcdConfigSpec(), newEtcdWatchInput)
	if err != nil {
		panic(err)
	}
}

type etcdWatchInput struct {
	watchKey string // TODO: Potentially allow multiple keys/prefixes to be watched (multiplexing)

	client     *clientv3.Client
	clientConf *clientv3.Config

	watchCh      clientv3.WatchChan
	watchOptions []clientv3.OpOption
}

func getWatchOptionsFromConfig(parsedConf *service.ParsedConfig) ([]clientv3.OpOption, error) {
	var opts []clientv3.OpOption

	shouldAddToWatchOptions := func(should bool, option clientv3.OpOption) {
		if should {
			opts = append(opts, option)
		}
	}

	withPrefix, err := parsedConf.FieldBool(etcdOperationOptions, etcdWithPrefixField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withPrefix, clientv3.WithPrefix())

	withProgressNotify, err := parsedConf.FieldBool(etcdOperationOptions, etcdWatchWithProgressNotifyField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withProgressNotify, clientv3.WithProgressNotify())

	withCreatedNotify, err := parsedConf.FieldBool(etcdOperationOptions, etcdWatchWithCreatedNotifyField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withCreatedNotify, clientv3.WithCreatedNotify())

	withFilterPut, err := parsedConf.FieldBool(etcdOperationOptions, etcdWatchWithFilterPut)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withFilterPut, clientv3.WithFilterPut())

	withFilterDelete, err := parsedConf.FieldBool(etcdOperationOptions, etcdWatchWithFilterDelete)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withFilterDelete, clientv3.WithFilterDelete())

	withRange, err := parsedConf.FieldString(etcdOperationOptions, etcdWatchWithRangeField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withRange != "", clientv3.WithRange(withRange))

	return opts, nil
}

func newEtcdWatchInputFromConfig(parsedConf *service.ParsedConfig, mgr *service.Resources) (*etcdWatchInput, error) {
	config, err := newEtcdConfigFromParsed(parsedConf)
	if err != nil {
		return nil, err
	}

	watchKey, err := parsedConf.FieldString(etcdKeyField)
	if err != nil {
		return nil, err
	}

	opts, err := getWatchOptionsFromConfig(parsedConf)
	if err != nil {
		return nil, err
	}

	return &etcdWatchInput{
		clientConf:   config,
		watchKey:     watchKey,
		watchOptions: opts,
	}, nil
}

func (e *etcdWatchInput) Connect(ctx context.Context) error {
	client, err := newEtcdClientFromConfig(ctx, e.clientConf)
	if err != nil {
		return err
	}

	e.client = client
	e.watchCh = clientv3.NewWatcher(e.client).Watch(ctx, e.watchKey, e.watchOptions...)

	return nil
}

func (e *etcdWatchInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case resp, open := <-e.watchCh:
		if err := resp.Err(); err != nil {
			return nil, nil, err
		}

		if resp.Canceled {
			return nil, nil, service.ErrEndOfInput
		}

		if !open {
			return nil, nil, service.ErrNotConnected
		}

		msg := service.NewMessage(nil)

		eventsBytes, err := marshalEtcdEventsToJSON(resp.Events)
		if err != nil {
			return nil, nil, err
		}

		msg.SetBytes(eventsBytes)

		return msg, func(ctx context.Context, err error) error {
			return err
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (e *etcdWatchInput) Close(ctx context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}

	return nil
}
