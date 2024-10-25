package arrow

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/warpstreamlabs/bento/public/service"
)

func arrowProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Parsing").
		Summary(`Performs Avro based operations on messages based on a schema.`).Field(schemaConfig()).Field(service.NewStringEnumField("format", "arrow", "parquet"))

}

func schemaConfig() *service.ConfigField {
	return service.NewObjectListField("schema",
		service.NewStringField("name").Description("The name of the column."),
		service.NewStringEnumField("type", "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "BYTE_ARRAY", "UTF8").
			Description("The type of the column, only applicable for leaf columns with no child fields. Some logical types can be specified here such as UTF8.").Optional(),
		service.NewBoolField("repeated").Description("Whether the field is repeated.").Default(false),
		service.NewBoolField("optional").Description("Whether the field is optional.").Default(false),
		service.NewAnyListField("fields").Description("A list of child fields.").Optional().Example([]any{
			map[string]any{
				"name": "foo",
				"type": "INT64",
			},
			map[string]any{
				"name": "bar",
				"type": "BYTE_ARRAY",
			},
		}),
	).Description("Parquet schema.")
}

func init() {
	err := service.RegisterBatchProcessor(
		"arrow", arrowProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newArrowFromConifg(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type arrowProcessor struct {
	schema *arrow.Schema
}

func newArrowFromConifg(conf *service.ParsedConfig, mgr *service.Resources) (*arrowProcessor, error) {
	schemaConfs, err := conf.FieldObjectList("schema")
	if err != nil {
		return nil, err
	}

	arrowSchema, err := fieldToArrowSchema(schemaConfs)
	if err != nil {
		return nil, fmt.Errorf("failed to construct Arrow schema from config: %w", err)
	}

	// TODO: Handle parquet format
	_, err = conf.FieldString("format")
	if err != nil {
		return nil, err
	}

	// switch format {
	// case "parquet":
	// 	parquetSchema, err := pqarrow.ToParquet(arrowSchema, nil, pqarrow.DefaultWriterProps())
	// 	if err != nil {
	// 		return nil, fmt.Errorf("failed to convert arrow to parquet schema: %w", err)
	// 	}

	// }

	return &arrowProcessor{
		schema: arrowSchema,
	}, nil

}

func (a *arrowProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var buffer bytes.Buffer
	for i, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("getting message bytes at index %d: %w", i, err)
		}

		if i > 0 {
			buffer.WriteByte('\n')
		}
		buffer.Write(msgBytes)
	}

	rdr := array.NewJSONReader(bytes.NewReader(buffer.Bytes()), a.schema)
	defer rdr.Release()

	i := 0
	if !rdr.Next() {
		if err := rdr.Err(); err != nil {
			return nil, fmt.Errorf("reading JSON data: %w", err)
		}
		record := rdr.Record()
		defer record.Release()

		batch[i].SetStructured(record)
		i++
	}

	return []service.MessageBatch{batch}, nil
}

func (a *arrowProcessor) Close(ctx context.Context) error {
	return nil
}
