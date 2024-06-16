package etcd

import (
	"context"
	"errors"

	"github.com/warpstreamlabs/bento/public/service"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func etcdWatchFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(etcdWatchKeyField).
			Description("The key or prefix being watched.").
			Default(""),
		service.NewObjectField(etcdOperationOptions,
			service.NewBoolField(etcdWatchWithPrefixField).
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
		Fields(etcdWatchFields()...)

	return spec
}

func init() {
	err := service.RegisterInput(
		etcdWatchField, etcdConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			reader, err := newEtcdWatchInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return reader, nil
		})
	if err != nil {
		panic(err)
	}
}

type etcdWatchInput struct {
	client       *etcdClient
	watchKey     string // TODO: Potentially allow multiple keys/prefixes to be watched (multiplexing)
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

	withPrefix, err := parsedConf.FieldBool(etcdWatchField, etcdWatchWithPrefixField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withPrefix, clientv3.WithPrefix())

	withProgressNotify, err := parsedConf.FieldBool(etcdWatchField, etcdWatchWithProgressNotifyField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withProgressNotify, clientv3.WithProgressNotify())

	withCreatedNotify, err := parsedConf.FieldBool(etcdWatchField, etcdWatchWithCreatedNotifyField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withCreatedNotify, clientv3.WithCreatedNotify())

	withFilterPut, err := parsedConf.FieldBool(etcdWatchField, etcdWatchWithFilterPut)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withFilterPut, clientv3.WithFilterPut())

	withFilterDelete, err := parsedConf.FieldBool(etcdWatchField, etcdWatchWithFilterDelete)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withFilterDelete, clientv3.WithFilterDelete())

	withRange, err := parsedConf.FieldString(etcdWatchField, etcdWatchWithRangeField)
	if err != nil {
		return nil, err
	}
	shouldAddToWatchOptions(withRange != "", clientv3.WithRange(withRange))

	return opts, nil
}

func newEtcdWatchInputFromConfig(parsedConf *service.ParsedConfig, mgr *service.Resources) (*etcdWatchInput, error) {
	client, err := newEtcdConfigFromParsed(parsedConf, mgr)
	if err != nil {
		return nil, err
	}

	watchKey, err := parsedConf.FieldString(etcdWatchField, etcdWatchKeyField)
	if err != nil {
		return nil, err
	}

	opts, err := getWatchOptionsFromConfig(parsedConf)
	if err != nil {
		return nil, err
	}

	return &etcdWatchInput{
		client:       client,
		watchKey:     watchKey,
		watchOptions: opts,
	}, nil
}

func (e *etcdWatchInput) Connect(ctx context.Context) error {
	if err := e.client.Connect(ctx); err != nil {
		return err
	}

	if e.client.cli == nil {
		return errors.New("etcd client is nil")
	}

	// TODO: this should be changed to start reading immediately
	e.watchCh = clientv3.NewWatcher(e.client.cli).Watch(ctx, e.watchKey, e.watchOptions...)

	return nil
}

func (e *etcdWatchInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	// TODO: Should this be a BatchReader?
	ack := func(ctx context.Context, err error) error {
		return nil
	}

	select {
	case resp := <-e.watchCh:
		if err := resp.Err(); err != nil {
			return nil, nil, err
		}
		msg := service.NewMessage(nil)
		msg.SetStructured(resp.Events)
		return msg, ack, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (e *etcdWatchInput) Close(ctx context.Context) error {
	if e.client != nil {
		return e.client.Close(ctx)
	}

	return nil
}
