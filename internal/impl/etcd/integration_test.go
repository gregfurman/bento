package etcd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestIntegrationEtcd(t *testing.T) {
}

func startETCDServer(t *testing.T) (endpoint string, close func()) {
	cfg := embed.NewConfig()
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"/dev/null"}
	cfg.Dir = filepath.Join(os.TempDir(), fmt.Sprint(time.Now().Nanosecond()))

	srv, err := embed.StartEtcd(cfg)
	assert.Nil(t, err)

	select {
	case <-srv.Server.ReadyNotify():
	case <-time.After(3 * time.Second):
		t.Fatalf("Failed to start embed.Etcd for tests")
	}

	return cfg.AdvertiseClientUrls[0].String(), func() {
		os.RemoveAll(cfg.Dir)
		srv.Close()
	}
}
