//go:build huggingbento

package huggingface

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/knights-analytics/hugot"
	"github.com/stretchr/testify/require"
	_ "github.com/warpstreamlabs/bento/public/components/io"
	_ "github.com/warpstreamlabs/bento/public/components/pure"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
	"github.com/zeebo/assert"
)

func TestIntegration_TextClassifier(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	tmpDir := t.TempDir()

	onnxLibPath := os.Getenv("ONNXRUNTIME_SHARED_LIB_PATH")

	session, err := globalSession.NewSession(onnxLibPath)
	require.NoError(t, err)

	modelName := "KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english"
	modelPath, err := session.DownloadModel(modelName, tmpDir, hugot.NewDownloadOptions())
	require.NoError(t, err)

	defer t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(tmpDir))
	})

	template := fmt.Sprintf(`
huggingface_text_classifer:
  pipeline_name: classify-incoming-data-1
  onnx_library_path: %s
  model_name: %s
  model_path: %s
`, onnxLibPath, modelName, modelPath)

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: INFO"))
	require.NoError(t, b.AddProcessorYAML(template))

	outBatches := map[string]struct{}{}
	var outMut sync.Mutex
	handler := func(_ context.Context, mb service.MessageBatch) error {
		outMut.Lock()
		defer outMut.Unlock()

		outMsgs := []string{}
		for _, m := range mb {
			b, err := m.AsBytes()
			assert.NoError(t, err)
			outMsgs = append(outMsgs, string(b))
		}

		outBatches[strings.Join(outMsgs, ",")] = struct{}{}
		return nil
	}
	require.NoError(t, b.AddBatchConsumerFunc(handler))

	pushFn, err := b.AddBatchProducerFunc()

	strm, err := b.Build()
	require.NoError(t, err)

	promptsBatch := [][]string{
		{"Bento boxes taste amazing!", "Meow meow meow... meow meow."},
		{"Why does the blobfish look so sad? :(", "Sir, are you aware of the magnificent octopus on your head?"},
		{"Streaming data is my favourite pastime.", "You are wearing a silly hat."},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		defer done()

		for _, prompts := range promptsBatch {
			batch := make(service.MessageBatch, len(prompts))
			for i, prompt := range prompts {
				batch[i] = service.NewMessage([]byte(prompt))
			}
			require.NoError(t, pushFn(ctx, batch))
		}

		require.NoError(t, strm.StopWithin(time.Second*5))
	}()

	require.NoError(t, strm.Run(context.Background()))
	wg.Wait()

	outMut.Lock()
	assert.Equal(t, map[string]struct{}{
		`[{"Label":"POSITIVE","Score":0.999869}],[{"Label":"POSITIVE","Score":0.9992634}]`:  {},
		`[{"Label":"NEGATIVE","Score":0.9996588}],[{"Label":"POSITIVE","Score":0.9908547}]`: {},
		`[{"Label":"POSITIVE","Score":0.9811118}],[{"Label":"NEGATIVE","Score":0.9700846}]`: {},
	}, outBatches)
	outMut.Unlock()

}
