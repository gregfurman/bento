package gcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"
)

func gcpBigQueryConfFromYAML(t *testing.T, yamlStr string) gcpBigQueryOutputConfig {
	t.Helper()
	spec := gcpBigQueryConfig()
	parsedConf, err := spec.ParseYAML(yamlStr, nil)
	require.NoError(t, err)

	conf, err := gcpBigQueryOutputConfigFromParsed(parsedConf)
	require.NoError(t, err)

	return conf
}

func newBigQueryEmulator(t *testing.T) string {
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

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

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
		err := table.Create(context.Background(), metaData)
		require.NoError(t, err)

		return nil
	})

	require.NoError(t, err)

	_ = resource.Expire(900)

	return url
}

func TestNewGCPBigQueryOutputJsonNewLineOk(t *testing.T) {
	output, err := newGCPBigQueryOutput(gcpBigQueryOutputConfig{}, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
}

func TestNewGCPBigQueryOutputCsvDefaultConfigIsoOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, ",", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvDefaultConfigUtfOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, ",", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvCustomConfigIsoOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.FieldDelimiter = "¨"

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, "\xa8", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvCustomConfigUtfOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.FieldDelimiter = "¨"

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, "¨", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvHeaderIsoOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.Header = []string{"a", "â", "ã", "ä"}

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\"a\",\"\xe2\",\"\xe3\",\"\xe4\"", string(output.csvHeaderBytes))
}

func TestNewGCPBigQueryOutputCsvHeaderUtfOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Header = []string{"a", "â", "ã", "ä"}

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\"a\",\"â\",\"ã\",\"ä\"", string(output.csvHeaderBytes))
}

func TestNewGCPBigQueryOutputCsvFieldDelimiterIsoError(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.FieldDelimiter = "\xa8"

	_, err := newGCPBigQueryOutput(config, nil)

	require.Error(t, err)
}

func TestNewGCPBigQueryOutputCsvHeaderIsoError(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.Header = []string{"\xa8"}

	_, err := newGCPBigQueryOutput(config, nil)

	require.Error(t, err)
}

func TestGCPBigQueryOutputConvertToIsoOk(t *testing.T) {
	value := "\"a\"¨\"â\"¨\"ã\"¨\"ä\""

	result, err := convertToIso([]byte(value))

	require.NoError(t, err)
	require.Equal(t, "\"a\"\xa8\"\xe2\"\xa8\"\xe3\"\xa8\"\xe4\"", string(result))
}

func TestGCPBigQueryOutputConvertToIsoError(t *testing.T) {
	value := "\xa8"

	_, err := convertToIso([]byte(value))
	require.Error(t, err)
}

func TestGCPBigQueryOutputCreateTableLoaderOk(t *testing.T) {
	url := newBigQueryEmulator(t)

	// Setting non-default values
	outputConfig := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
write_disposition: WRITE_TRUNCATE
create_disposition: CREATE_NEVER
format: CSV
auto_detect: true
ignore_unknown_values: true
max_bad_records: 123
csv:
  field_delimiter: ';'
  allow_jagged_rows: true
  allow_quoted_newlines: true
  encoding: ISO-8859-1
  skip_leading_rows: 10
`)

	output, err := newGCPBigQueryOutput(outputConfig, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(url)
	err = output.Connect(context.Background())
	defer output.Close(context.Background())
	require.NoError(t, err)

	data := []byte("1,2,3")
	loader := output.createTableLoader(&data)

	assert.Equal(t, "table_meow", loader.Dst.TableID)
	assert.Equal(t, "dataset_meow", loader.Dst.DatasetID)
	assert.Equal(t, "project_meow", loader.Dst.ProjectID)
	assert.Equal(t, bigquery.TableWriteDisposition(outputConfig.WriteDisposition), loader.WriteDisposition)
	assert.Equal(t, bigquery.TableCreateDisposition(outputConfig.CreateDisposition), loader.CreateDisposition)

	readerSource, ok := loader.Src.(*bigquery.ReaderSource)
	require.True(t, ok)

	assert.Equal(t, bigquery.DataFormat(outputConfig.Format), readerSource.SourceFormat)
	assert.Equal(t, outputConfig.AutoDetect, readerSource.AutoDetect)
	assert.Equal(t, outputConfig.IgnoreUnknownValues, readerSource.IgnoreUnknownValues)
	assert.Equal(t, int64(outputConfig.MaxBadRecords), readerSource.MaxBadRecords)

	expectedCsvOptions := outputConfig.CSVOptions

	assert.Equal(t, expectedCsvOptions.FieldDelimiter, readerSource.FieldDelimiter)
	assert.Equal(t, expectedCsvOptions.AllowJaggedRows, readerSource.AllowJaggedRows)
	assert.Equal(t, expectedCsvOptions.AllowQuotedNewlines, readerSource.AllowQuotedNewlines)
	assert.Equal(t, bigquery.Encoding(expectedCsvOptions.Encoding), readerSource.Encoding)
	assert.Equal(t, int64(expectedCsvOptions.SkipLeadingRows), readerSource.SkipLeadingRows)
}

func TestGCPBigQueryOutputDatasetDoNotExists(t *testing.T) {
	url := newBigQueryEmulator(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_woof
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(url)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.EqualError(t, err, "dataset does not exist: dataset_woof")
}

func TestGCPBigQueryOutputDatasetDoNotExistsUnknownError(t *testing.T) {
	// TODO: Not sure this is a worthwhile testcase?
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer done()

	err = output.Connect(ctx)
	defer output.Close(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "googleapi: got HTTP response code 500 with body: {}")
}

func TestGCPBigQueryOutputTableDoNotExists(t *testing.T) {
	url := newBigQueryEmulator(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_woof
create_disposition: CREATE_NEVER
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(url)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "table does not exist: table_woof")
}

func TestGCPBigQueryOutputTableDoNotExistsUnknownError(t *testing.T) {
	// TODO: here as well
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))

				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
create_disposition: CREATE_NEVER
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer done()

	err = output.Connect(ctx)
	defer output.Close(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "googleapi: got HTTP response code 500 with body: {}")
}

func TestGCPBigQueryOutputConnectOk(t *testing.T) {
	url := newBigQueryEmulator(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(url)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)
}

func TestGCPBigQueryOutputConnectWithoutTableOk(t *testing.T) {
	url := newBigQueryEmulator(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(url)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)
}

func TestGCPBigQueryOutputWriteOk(t *testing.T) {
	url := newBigQueryEmulator(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

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

	dataset := output.client.Dataset(datasetID)
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
}

func TestGCPBigQueryOutputWriteError(t *testing.T) {
	url := newBigQueryEmulator(t)

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
}
