package etcd

import (
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service/integration"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestIntegrationEtcd(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "bitnami/etcd",
		Tag:          "latest",
		ExposedPorts: []string{"2379", "2380"},
		Env: []string{
			"ALLOW_NONE_AUTHENTICATION=yes",
			"ETCD_ADVERTISE_CLIENT_URLS=http://localhost:2379",
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	var cli *clientv3.Client
	require.NoError(t, pool.Retry(func() (err error) {
		defer func() {
			if err != nil {
				t.Logf("error: %v", err)
			}
		}()

		cli, err = clientv3.New(clientv3.Config{
			Endpoints: []string{"http://localhost:2379"},
		})

		return err
	}))

	defer t.Cleanup(func() {
		cli.Close()
	})

	etcdWatchIntegrationSuite(t)
	// _, err = cli.Put(context.Background(), "foo", `{ "bar" : "baz" }`)

}

func etcdWatchIntegrationSuite(t *testing.T) {

	t.Run("watches_single_key", func(t *testing.T) {
		template := `
input:
	etcd:
	key: "foo"
	endpoints:
		- "http://localhost:2379"`

		// streamOutBuilder := service.NewStreamBuilder()
		// require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
		// require.NoError(t, streamOutBuilder.AddInputYAML(template))

		suite := integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestStreamParallel(1000),
		)

		suite.Run(t, template)
	})

	t.Run("watches_all_keys", func(t *testing.T) {
		template := `
input:
	etcd:
	key: ""
	endpoints:
		- "http://localhost:2379"
	options:
      with_prefix: true`

		suite := integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestSendBatch(10),
			integration.StreamTestStreamParallel(1000),
		)

		suite.Run(t, template)

	})

}
