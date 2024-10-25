package arrow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/warpstreamlabs/bento/public/service"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestConvertConfigToArrow(t *testing.T) {
	var (
		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
			{
				Name: "f3", Type: arrow.StructOf(
					[]arrow.Field{
						{Name: "f3_1", Type: arrow.BinaryTypes.String, Nullable: true},
						{Name: "f3_2", Type: arrow.BinaryTypes.String, Nullable: true},
						{Name: "f3_3", Type: arrow.BinaryTypes.String, Nullable: false},
					}...,
				),
			},
		}
		expectedType = arrow.NewSchema(fields, nil)
	)

	encodeConf, err := arrowProcessorConfigSpec().ParseYAML(`
schema:
  - { name: f1, type: DOUBLE, optional: true }
  - { name: f2, type: INT32 }
  - name: f3
    fields:
      - { name: f3_1, type: UTF8, optional: true }
      - { name: f3_2, type: UTF8, optional: true }
      - { name: f3_3, type: UTF8 }

`, nil)
	require.NoError(t, err)

	arrowProc, err := newArrowFromConifg(encodeConf, nil)
	require.NoError(t, err)

	require.Equal(t, *expectedType, *arrowProc.schema)

	jsonStr := `{"f1": 4.2, "f2": 3, "f3": {"f3_3": "test"}}`

	message := service.NewMessage(nil)
	message.SetBytes([]byte(jsonStr))

	out, err := arrowProc.ProcessBatch(context.Background(), service.MessageBatch{message})
	require.NoError(t, err)

	_, err = out[0][0].AsStructured()
	require.NoError(t, err)
}

func TestConvertConfigWithArrayToArrow(t *testing.T) {
	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "intField", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: false},
		{Name: "stringField", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
		{Name: "floatField", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64), Nullable: true},
	}, nil)

	encodeConf, err := arrowProcessorConfigSpec().ParseYAML(`
schema:
  - { name: intField, type: INT64, repeated: true }
  - { name: stringField, type: STRING, repeated: true }
  - { name: floatField, type: FLOAT64, repeated: true, optional: true }
`, nil)
	require.NoError(t, err)

	arrowProc, err := newArrowFromConifg(encodeConf, nil)
	require.NoError(t, err)

	require.Equal(t, *expectedSchema, *arrowProc.schema)

}
func TestStructArrayUnmarshalJSONMissingFields(t *testing.T) {
	pool := memory.NewGoAllocator()

	var (
		fields = []arrow.Field{
			{Name: "f1", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
			{
				Name: "f3", Type: arrow.StructOf(
					[]arrow.Field{
						{Name: "f3_1", Type: arrow.BinaryTypes.String, Nullable: true},
						{Name: "f3_2", Type: arrow.BinaryTypes.String, Nullable: true},
						{Name: "f3_3", Type: arrow.BinaryTypes.String, Nullable: false},
					}...,
				),
			},
		}
		dtype = arrow.StructOf(fields...)
	)

	tests := []struct {
		name      string
		jsonInput string
		want      string
		panic     bool
	}{
		{
			name:      "missing required field",
			jsonInput: `[{"f2": 3, "f3": {"f3_1": "test"}}]`,
			panic:     true,
			want:      "",
		},
		{
			name:      "missing optional fields",
			jsonInput: `[{"f2": 3, "f3": {"f3_3": "test"}}]`,
			panic:     false,
			want:      `{[(null)] [3] {[(null)] [(null)] ["test"]}}`,
		},
	}

	for _, tc := range tests {
		t.Run(
			tc.name, func(t *testing.T) {

				var val bool

				sb := array.NewStructBuilder(pool, dtype)
				defer sb.Release()

				if tc.panic {
					defer func() {
						e := recover()
						if e == nil {
							t.Fatalf("this should have panicked, but did not; slice value %v", val)
						}
						if got, want := e.(string), "arrow/array: index out of range"; got != want {
							t.Fatalf("invalid error. got=%q, want=%q", got, want)
						}
					}()
				} else {
					defer func() {
						if e := recover(); e != nil {
							t.Fatalf("unexpected panic: %v", e)
						}
					}()
				}

				err := sb.UnmarshalJSON([]byte(tc.jsonInput))
				if err != nil {
					t.Fatal(err)
				}

				arr := sb.NewArray().(*array.Struct)
				defer arr.Release()

				got := arr.String()
				if got != tc.want {
					t.Fatalf("invalid string representation:\ngot = %q\nwant= %q", got, tc.want)
				}

			},
		)
	}
}
