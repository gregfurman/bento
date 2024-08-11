package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	_ "github.com/warpstreamlabs/bento/internal/impl/io"
	_ "github.com/warpstreamlabs/bento/internal/impl/pure"
)

const (
	datasetID = "dataset_meow"
	projectID = "project_meow"
	tableID   = "table_meow"
)

// Item represents a row in the emulated table in BigQuery
type Item struct {
	what1 string
	what2 int
	what3 bool
}

// Save() allows Item to implement the BigQuery.ValueSaver interface for testing purposes
func (i *Item) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"what1": i.what1,
		"what2": i.what2,
		"what3": i.what3,
	}, bigquery.NoDedupeID, nil
}

func TestIntegrationGBQ(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "ghcr.io/goccy/bigquery-emulator",
		Tag:          "latest",
		ExposedPorts: []string{"9050/tcp", "9060/tcp"},
		Cmd:          []string{"--project", projectID, "--dataset", datasetID},
		Platform:     "linux/x86_64",
	})

	require.NoError(t, err)
	url := "http://localhost:" + resource.GetPort("9050/tcp")
	var client *bigquery.Client
	err = pool.Retry(func() error {
		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelFunc()

		var retryErr error

		client, retryErr = bigquery.NewClient(ctx, projectID, option.WithEndpoint(url), option.WithoutAuthentication())
		if err != nil {
			return retryErr
		}

		dataset := client.Dataset(datasetID)

		sampleSchema := bigquery.Schema{
			{Name: "what1", Type: bigquery.StringFieldType},
			{Name: "what2", Type: bigquery.IntegerFieldType},
			{Name: "what3", Type: bigquery.BooleanFieldType},
		}

		metaData := &bigquery.TableMetadata{
			Schema:         sampleSchema,
			ExpirationTime: time.Now().AddDate(1, 0, 0),
		}

		table := dataset.Table(tableID)
		if retryErr := table.Create(ctx, metaData); err != nil {
			return retryErr
		}

		return nil
	})

	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	t.Run("gcp_bigquery_write_batch", func(t *testing.T) {
		t.Parallel()

		tmpl := `
project: "project_meow"
dataset: "dataset_meow"
table: "table_meow"
`
		config := gcpBigQueryConfFromYAML(t, tmpl)
		output, err := newGCPBigQueryOutput(config, nil)
		require.NoError(t, err)

		output.clientURL = gcpBQClientURL(url)

		err = output.Connect(context.Background())
		defer output.Close(context.Background())

		require.NoError(t, err)

		err = output.WriteBatch(context.Background(), service.MessageBatch{
			service.NewMessage([]byte(`{"what1":"meow1","what2":1,"what3":true}`)),
			service.NewMessage([]byte(`{"what1":"meow2","what2":2,"what3":false}`)),
			service.NewMessage([]byte(`{"what1":"meow3","what2":3,"what3":true}` + "\n" + `{"what1":"meow4","what2":4,"what3":false}`)),
			service.NewMessage([]byte(`{"what1":"meow5","what2":5,"what3":true},{"what1":"meow6","what2":6,"what3":false}`)),
		})
		require.NoError(t, err)

		dataset := client.Dataset(datasetID)
		table := dataset.Table(tableID)
		it := table.Read(context.Background())
		require.NotNil(t, it)

		var out []string
		for {
			var row []bigquery.Value
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			require.NoError(t, err)
			content, err := json.Marshal(row)
			require.NoError(t, err)
			out = append(out, string(content))
		}

		expectedOut := []string{
			`["meow1",1,true]`, `["meow2",2,false]`,
			`["meow3",3,true]`, `["meow4",4,false]`,
			`["meow5",5,true]`, `["meow6",6,false]`}
		require.NoError(t, err)
		require.Equal(t, expectedOut, out)

	})

	t.Run("gcp_bigquery_write_error", func(t *testing.T) {
		t.Parallel()

		tmpl := `
project: "project_meow"
dataset: "dataset_meow"
table: "table_meow"
`
		config := gcpBigQueryConfFromYAML(t, tmpl)
		output, err := newGCPBigQueryOutput(config, nil)
		require.NoError(t, err)

		output.clientURL = gcpBQClientURL(url)

		err = output.Connect(context.Background())
		defer output.Close(context.Background())

		require.NoError(t, err)

		err = output.WriteBatch(context.Background(), service.MessageBatch{
			service.NewMessage([]byte(`{\"what1\":\"meow1\",\"what2\":1,\"what3\":true}`)),
		})
		require.Error(t, err)

		err = output.WriteBatch(context.Background(), service.MessageBatch{
			service.NewMessage([]byte(`[{"what1":"meow2","what2":2,"what3":false}]`)),
		})
		require.Error(t, err)

		err = output.WriteBatch(context.Background(), service.MessageBatch{
			service.NewMessage([]byte(`[ {"what1":"meow5","what2":5,"what3":true},` + "\n" + `{"what1":"meow6","what2":6,"what3":false}, ]`)),
		})
		require.Error(t, err)

		err = output.WriteBatch(context.Background(), service.MessageBatch{
			service.NewMessage([]byte(`\`)),
		})
		require.Error(t, err)

		err = output.WriteBatch(context.Background(), service.MessageBatch{
			service.NewMessage([]byte(`\"`)),
		})
		require.Error(t, err)

	})

	t.Run("gcp_biguery_select_input", func(t *testing.T) {
		t.Parallel()

		tmpl := fmt.Sprintf(`
project: "project_meow"
table: "project_meow.dataset_meow.table_meow"
endpoint: "http://localhost:%s"
columns: ["what1", "what2", "what3"]
`, resource.GetPort("9050/tcp"))

		// Populate DB
		inserter := client.Dataset(datasetID).Table(tableID).Inserter()

		items := []*Item{
			{what1: "meow1", what2: 1, what3: true},
			{what1: "meow2", what2: 2, what3: false},
		}
		require.NoError(t, inserter.Put(context.Background(), items))

		// Start selector
		spec := newBigQuerySelectInputConfig()

		parsed, err := spec.ParseYAML(tmpl, nil)
		require.NoError(t, err)

		inp, err := newBigQuerySelectInput(parsed, nil)
		require.NoError(t, err)

		err = inp.Connect(context.Background())
		defer inp.Close(context.Background())

		require.NoError(t, err)

		msg1, _, err := inp.Read(context.Background())
		require.NoError(t, err)

		msg2, _, err := inp.Read(context.Background())
		require.NoError(t, err)

		data1, err := msg1.AsBytes()
		require.NoError(t, err)

		data2, err := msg2.AsBytes()
		require.NoError(t, err)

		_, _, err = inp.Read(context.Background())
		require.Error(t, err)

		require.Equal(t, `{"what1":"meow1","what2":1,"what3":true}`, string(data1))
		require.Equal(t, `{"what1":"meow2","what2":2,"what3":false}`, string(data2))
	})

	t.Run("gcp_biguery_select_processor", func(t *testing.T) {
		t.Parallel()

		tmpl := fmt.Sprintf(`
project: "project_meow"
table: "project_meow.dataset_meow.table_meow"
endpoint: "http://localhost:%s"
columns: ["what2"]
where: what2 <= ?
args_mapping: root = [ this.which ]
`, resource.GetPort("9050/tcp"))

		// Populate DB
		inserter := client.Dataset(datasetID).Table(tableID).Inserter()

		items := []*Item{
			{what1: "meow1", what2: 1, what3: true},
			{what1: "meow2", what2: 2, what3: false},
			{what1: "meow3", what2: 3, what3: true},
			{what1: "meow4", what2: 4, what3: false},
		}
		require.NoError(t, inserter.Put(context.Background(), items))

		// Start selector
		spec := newBigQuerySelectInputConfig()

		parsed, err := spec.ParseYAML(tmpl, nil)
		require.NoError(t, err)

		proc, err := newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{})
		require.NoError(t, err)

		// Retrieve all values where what2 <= which
		inbatch := service.MessageBatch{
			service.NewMessage([]byte(`{"which": "2"}`)),
			service.NewMessage([]byte(`{"which": "4"}`)),
		}

		outBatch, err := proc.ProcessBatch(context.Background(), inbatch)
		require.NoError(t, err)

		var results []string
		for _, msg := range outBatch[0] {

			data, err := msg.AsBytes()
			require.NoError(t, err)

			results = append(results, string(data))

		}

		require.Equal(t, []string{`[{"what2":1},{"what2":2}]`, `[{"what2":1},{"what2":2},{"what2":3},{"what2":4}]`}, results)
	})

}
