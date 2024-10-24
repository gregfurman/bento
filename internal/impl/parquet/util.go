package parquet

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/warpstreamlabs/bento/public/service"
)

func scrubJSONNumbers(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return 0
	case map[string]any:
		scrubJSONNumbersObj(t)
		return t
	case []any:
		scrubJSONNumbersArr(t)
		return t
	}
	return v
}

func scrubJSONNumbersObj(obj map[string]any) {
	for k, v := range obj {
		obj[k] = scrubJSONNumbers(v)
	}
}

func scrubJSONNumbersArr(arr []any) {
	for i, v := range arr {
		arr[i] = scrubJSONNumbers(v)
	}
}

func goTypeOf(node parquet.Node) reflect.Type {
	switch {
	case node.Optional():
		return goTypeOfOptional(node)
	case node.Repeated():
		return goTypeOfRepeated(node)
	default:
		return goTypeOfRequired(node)
	}
}

func goTypeOfOptional(node parquet.Node) reflect.Type {
	return reflect.PtrTo(goTypeOfRequired(node))
}

func goTypeOfRepeated(node parquet.Node) reflect.Type {
	return reflect.SliceOf(goTypeOfRequired(node))
}

func goTypeOfRequired(node parquet.Node) reflect.Type {
	if node.Leaf() {
		return goTypeOfLeaf(node)
	} else {
		return goTypeOfGroup(node)
	}
}

func goTypeOfLeaf(node parquet.Node) reflect.Type {
	t := node.Type()
	if convertibleType, ok := t.(interface{ GoType() reflect.Type }); ok {
		return convertibleType.GoType()
	}
	switch t.Kind() {
	case parquet.Boolean:
		return reflect.TypeOf(false)
	case parquet.Int32:
		return reflect.TypeOf(int32(0))
	case parquet.Int64:
		return reflect.TypeOf(int64(0))
	case parquet.Float:
		return reflect.TypeOf(float32(0))
	case parquet.Double:
		return reflect.TypeOf(float64(0))
	case parquet.ByteArray:
		return reflect.TypeOf(([]byte)(nil))
	case parquet.FixedLenByteArray:
		return reflect.ArrayOf(t.Length(), reflect.TypeOf(byte(0)))
	default:
		panic("BUG: parquet type returned an unsupported kind")
	}
}

func exportedStructFieldName(name string) string {
	firstRune, size := utf8.DecodeRuneInString(name)
	return string([]rune{unicode.ToUpper(firstRune)}) + name[size:]
}

func isList(node parquet.Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.List != nil
}

func isMap(node parquet.Node) bool {
	logicalType := node.Type().LogicalType()
	return logicalType != nil && logicalType.Map != nil
}

func goTypeOfGroup(node parquet.Node) reflect.Type {
	fields := node.Fields()
	structFields := make([]reflect.StructField, len(fields))

	for i, field := range fields {
		var tags []string

		structFields[i].Name = exportedStructFieldName(field.Name())
		structFields[i].Type = field.GoType()

		tags = append(tags, field.Name())

		if field.Optional() {
			tags = append(tags, "optional")
		}

		if isList(field) {
			tags = append(tags, "list")
		}

		switch field.Compression() {
		case &parquet.Snappy:
			tags = append(tags, "snappy")
		case &parquet.Gzip:
			tags = append(tags, "gzip")
		case &parquet.Brotli:
			tags = append(tags, "brotli")
		case &parquet.Zstd:
			tags = append(tags, "zstd")
		case &parquet.Lz4Raw:
			tags = append(tags, "lz4raw")
		}

		if field.Encoding() == &parquet.Plain {
			tags = append(tags, "plain")
		}

		structFields[i].Tag = reflect.StructTag(fmt.Sprintf(`parquet:"%v"`, strings.Join(tags, ",")))
		// TODO: can we reconstruct a struct tag that would be valid if a value
		// of this type were passed to SchemaOf?
	}
	return reflect.StructOf(structFields)
}

// TODO: Add support for field level compression and encoding. Currently, we set all encoding/compression for fields to be the same.
func createStructType(columnConfs []*service.ParsedConfig, encodingTag, compressionTag string) (reflect.Type, error) {
	var fields []reflect.StructField

	for _, colConf := range columnConfs {
		name, err := colConf.FieldString("name")
		if err != nil {
			return nil, err
		}

		field, err := parquetStructFromConfig(colConf, encodingTag, compressionTag)
		if err != nil {
			return nil, err
		}

		field.Tag = reflect.StructTag(fmt.Sprintf(`parquet:"%s" json:"%s"`, name, name))
		fields = append(fields, field)
	}

	return reflect.StructOf(fields), nil
}

func parquetStructFromConfig(colConf *service.ParsedConfig, encodingTag, compressionTag string) (reflect.StructField, error) {
	name, err := colConf.FieldString("name")
	if err != nil {
		return reflect.StructField{}, err
	}

	hasType := colConf.Contains("type")
	childColumns, _ := colConf.FieldAnyList("fields")

	isMap := hasType && len(childColumns) == 2
	isNested := !hasType && len(childColumns) > 0
	isTerminal := hasType && len(childColumns) == 0

	var fieldType reflect.Type

	switch {
	case isNested:
		nestedType, err := createStructType(childColumns, encodingTag, compressionTag)
		if err != nil {
			return reflect.StructField{}, err
		}
		fieldType = nestedType

	case isTerminal:
		typeStr, err := colConf.FieldString("type")
		if err != nil {
			return reflect.StructField{}, err
		}
		fieldType, err = createBasicStructField(name, typeStr)
		if err != nil {
			return reflect.StructField{}, err
		}

	case isMap:
		typeStr, err := colConf.FieldString("type")
		if err != nil {
			return reflect.StructField{}, err
		}
		if typeStr != "MAP" {
			return reflect.StructField{}, fmt.Errorf("invalid field %v of type %s: only a MAP can have child fields", name, typeStr)
		}
		fieldType, err = createMapStructField(name, childColumns, encodingTag, compressionTag)
		if err != nil {
			return reflect.StructField{}, err
		}
	}

	repeated, _ := colConf.FieldBool("repeated")
	optional, _ := colConf.FieldBool("optional")

	if repeated && optional {
		return reflect.StructField{}, fmt.Errorf("column %v cannot be both repeated and optional", name)
	}

	if repeated {
		fieldType = reflect.SliceOf(fieldType)
	}

	if optional {
		fieldType = reflect.PtrTo(fieldType)
	}

	return reflect.StructField{
		Name: exportedStructFieldName(name), // has to be uppercase
		Type: fieldType,
		Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"%s"`, name)),
	}, nil
}

func createBasicStructField(name, typeStr string) (reflect.Type, error) {
	switch typeStr {
	case "BOOLEAN":
		return reflect.TypeOf(false), nil
	case "INT32":
		return reflect.TypeOf(int32(0)), nil
	case "INT64":
		return reflect.TypeOf(int64(0)), nil
	case "FLOAT":
		return reflect.TypeOf(float32(0)), nil
	case "DOUBLE":
		return reflect.TypeOf(float64(0)), nil
	case "BYTE_ARRAY":
		return reflect.TypeOf([]byte(nil)), nil
	case "UTF8":
		return reflect.TypeOf(""), nil
	default:
		return nil, fmt.Errorf("field %v type of '%v' not recognised", name, typeStr)
	}
}

func createMapStructField(name string, mapFields []*service.ParsedConfig, encodingTag, compressionTag string) (reflect.Type, error) {
	if len(mapFields) != 2 {
		return nil, fmt.Errorf("field %v of type MAP must have exactly two fields", name)
	}

	var keyField, valueField *service.ParsedConfig

	for _, field := range mapFields {
		colName, err := field.FieldString("name")
		if err != nil {
			return nil, err
		}
		switch colName {
		case "key":
			keyField = field
		case "value":
			valueField = field
		default:
			return nil, fmt.Errorf("invalid naming of field %s of MAP can only be named 'key' or 'value'", colName)
		}
	}

	if keyField == nil || valueField == nil {
		return nil, fmt.Errorf("failed to parse field %s required 'key' and 'value' fields from MAP config", name)
	}

	keyType, err := parquetStructFromConfig(keyField, encodingTag, compressionTag)
	if err != nil {
		return nil, err
	}

	valueType, err := parquetStructFromConfig(valueField, encodingTag, compressionTag)
	if err != nil {
		return nil, err
	}

	return reflect.MapOf(keyType.Type, valueType.Type), nil
}
