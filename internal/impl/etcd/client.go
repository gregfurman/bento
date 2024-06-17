package etcd

import (
	"context"

	"github.com/warpstreamlabs/bento/public/service"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func etcdClientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewURLListField(etcdEndpointsField).
			Description("A set of URLs (schemes, hosts and ports only) that can be used to communicate with a logical etcd cluster. If multiple endpoints are provided, the Client will attempt to use them all in the event that one or more of them are unusable.").
			Examples(
				[]string{"etcd://:2379"},
				[]string{"etcd://localhost:2379"},
				[]string{"etcd://localhost:2379", "etcd://localhost:22379", "etcd://localhost:32379"},
			),
		service.NewObjectField(etcdAuthField,
			service.NewBoolField(etcdAuthEnabledField).
				Description("Whether to use password authentication").
				Default(false),
			service.NewStringField(etcdAuthUsernameField).
				Description("The username to authenticate as.").
				Default(""),
			service.NewStringField(etcdAuthPasswordField).
				Description("The password to authenticate with.").
				Secret().
				Default(""),
		).
			Description("Optional configuration of etcd authentication headers.").
			Advanced(),
		service.NewDurationField(etcdDialTimeoutField).
			Description("Timeout for failing to establish a connection.").
			Optional().
			Default("5s").
			Advanced(),
		service.NewDurationField(etcdKeepAliveTimeField).
			Description("Time after which client pings the server to see if transport is alive.").
			Optional().
			Default("5s").
			Advanced(),
		service.NewDurationField(etcdKeepAliveTimoutField).
			Description("Time that the client waits for a response for the keep-alive probe. If the response is not received in this time, the connection is closed.").
			Optional().
			Default("1s").
			Advanced(),
		service.NewDurationField(etcdRequestTimeoutField).
			Description("Timeout for a single request. This includes connection time, any redirects, and header wait time.").
			Optional().
			Default("1s").
			Advanced(),
		service.NewTLSToggledField(etcdTlsField).
			Description("Custom TLS settings can be used to override system defaults.").
			Advanced(),
	}
}

type etcdClient struct {
	cli    *clientv3.Client
	config *clientv3.Config

	logger *service.Logger
}

func (e *etcdClient) Connect(ctx context.Context) error {
	e.config.Context = ctx

	client, err := clientv3.New(*e.config)
	if err != nil {
		return err
	}

	e.cli = client

	return nil
}

func (e *etcdClient) Close(_ context.Context) error {
	return e.cli.Close()
}

func newEtcdConfigFromParsed(parsedConf *service.ParsedConfig, mgr *service.Resources) (*etcdClient, error) {
	endpointStrs, err := parsedConf.FieldStringList(etcdEndpointsField)
	if err != nil {
		return nil, err
	}

	dialTimeout, err := parsedConf.FieldDuration(etcdDialTimeoutField)
	if err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := parsedConf.FieldTLSToggled(etcdTlsField)
	if err != nil {
		return nil, err
	}
	if !tlsEnabled {
		tlsConf = nil
	}

	cfg := &clientv3.Config{
		Endpoints:   endpointStrs,
		DialTimeout: dialTimeout,
		TLS:         tlsConf,
	}

	return &etcdClient{
		config: cfg,
		logger: mgr.Logger(),
	}, nil

}
