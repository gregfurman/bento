package etcd

import (
	"context"
	"github.com/warpstreamlabs/bento/public/service"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func etcdClientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewURLListField(endpointsField).
			Description("A set of URLs (schemes, hosts and ports only) that can be used to communicate with a logical etcd cluster. If multiple endpoints are provided, the Client will attempt to use them all in the event that one or more of them are unusable.").
			Examples(
				[]string{"etcd://:2379"},
				[]string{"etcd://localhost:2379"},
				[]string{"etcd://localhost:2379", "etcd://localhost:22379", "etcd://localhost:32379"},
			),
		service.NewObjectField(authField,
			service.NewBoolField(authEnabledField).
				Description("Whether to use password authentication").
				Default(false),
			service.NewStringField(authUsernameField).
				Description("The username to authenticate as.").
				Default(""),
			service.NewStringField(authPasswordField).
				Description("The password to authenticate with.").
				Secret().
				Default(""),
		).
			Description("Optional configuration of etcd authentication headers.").
			Advanced(),
		service.NewDurationField(dialTimeoutField).
			Description("Timeout for failing to establish a connection.").
			Optional(),
		service.NewDurationField(keepAliveTimeField).
			Description("Time after which client pings the server to see if transport is alive.").
			Optional(),
		service.NewDurationField(keepAliveTimoutField).
			Description("Time that the client waits for a response for the keep-alive probe. If the response is not received in this time, the connection is closed.").
			Optional(),
		service.NewDurationField(requestTimeoutField).
			Description("Timeout for a single request. This includes connection time, any redirects, and header wait time."),
		service.NewTLSToggledField(tlsField).
			Description("Custom TLS settings can be used to override system defaults."),
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
	endpointStrs, err := parsedConf.FieldStringList(endpointsField)
	if err != nil {
		return nil, err
	}

	dialTimeout, err := parsedConf.FieldDuration(dialTimeoutField)
	if err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := parsedConf.FieldTLSToggled(tlsField)
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
