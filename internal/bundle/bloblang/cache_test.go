package bloblang_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	bundle "github.com/warpstreamlabs/bento/internal/bundle/bloblang"
	"github.com/warpstreamlabs/bento/internal/component/testutil"
	"github.com/warpstreamlabs/bento/internal/manager"
	"github.com/warpstreamlabs/bento/internal/message"

	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

func TestCacheResourceBloblang(t *testing.T) {

	mgrConf, err := testutil.ManagerFromYAML(`
cache_resources:
  - label: foo
    memory: {}
`)
	require.NoError(t, err)

	pConf, err := testutil.ProcessorFromYAML(`
processors:
  - bloblang: cache_set("foo", content(), content() + "_cached")
  - bloblang: cache_get("foo", content())
`)
	require.NoError(t, err)

	mgr, err := manager.New(mgrConf)
	require.NoError(t, err)

	bundle.SetManagementSingleton(mgr)

	proc, err := mgr.NewProcessor(pConf)
	require.NoError(t, err)

	ctx := context.Background()

	batch := message.QuickBatch([][]byte{
		[]byte("msg_1"),
		[]byte("msg_2"),
		[]byte("msg_3"),
	})

	out, err := proc.ProcessBatch(ctx, batch)
	require.NoError(t, err)
	require.Len(t, out[0], 3)
	require.Equal(t, "msg_1", string(out[0][0].AsBytes()))
	require.Equal(t, "msg_2", string(out[0][1].AsBytes()))
	require.Equal(t, "msg_3", string(out[0][2].AsBytes()))

}
