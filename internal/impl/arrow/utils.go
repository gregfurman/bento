package arrow

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/warpstreamlabs/bento/public/service"
)

func fieldToArrowSchema(colConfs []*service.ParsedConfig) (*arrow.Schema, error) {
	fields, err := convertFields(colConfs)
	if err != nil {
		return nil, err
	}
	return arrow.NewSchema(fields, nil), nil
}

func convertFields(cols []*service.ParsedConfig) ([]arrow.Field, error) {
	result := make([]arrow.Field, 0, len(cols))
	for _, col := range cols {
		name, err := col.FieldString("name")
		if err != nil {
			return nil, err
		}
		field, err := convertField(col)
		if err != nil {
			return nil, fmt.Errorf("unable to convert field %s from config to arrow: %w", name, err)
		}
		result = append(result, field)
	}
	return result, nil
}

func convertField(colConf *service.ParsedConfig) (arrow.Field, error) {
	name, err := colConf.FieldString("name")
	if err != nil {
		return arrow.Field{}, err
	}

	repeated, err := colConf.FieldBool("repeated")
	if err != nil {
		repeated = false
	}

	childColumns, _ := colConf.FieldAnyList("fields")

	var fieldType arrow.DataType
	if len(childColumns) > 0 {
		children, err := convertFields(childColumns)
		if err != nil {
			return arrow.Field{}, err
		}

		structType := arrow.StructOf(children...)
		if repeated {
			fieldType = arrow.ListOf(structType)
		} else {
			fieldType = structType
		}
	} else {
		typ, err := colConf.FieldString("type")
		if err != nil {
			return arrow.Field{}, err
		}

		baseType, err := stringToArrowType(typ)
		if err != nil {
			return arrow.Field{}, err
		}

		if repeated {
			fieldType = arrow.ListOf(baseType)
		} else {
			fieldType = baseType
		}
	}

	optional, _ := colConf.FieldBool("optional")

	return arrow.Field{
		Name:     name,
		Type:     fieldType,
		Nullable: optional,
	}, nil
}

func stringToArrowType(typ string) (arrow.DataType, error) {
	switch strings.ToLower(typ) {
	case "string", "utf8":
		return arrow.BinaryTypes.String, nil
	case "int", "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "uint", "uint64":
		return arrow.PrimitiveTypes.Uint64, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "float64", "double":
		return arrow.PrimitiveTypes.Float64, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "timestamp":
		return arrow.FixedWidthTypes.Timestamp_ms, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	case "time":
		return arrow.FixedWidthTypes.Time32s, nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", typ)
	}
}
