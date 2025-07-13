package conduit

import (
	"errors"
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/warpstreamlabs/bento/public/service"
)

var errInvalidFormat = errors.New("cannot convert message to opencdc format")

const (
	metaRecordPosition  = "position"
	metaRecordKey       = "key"
	metaRecordOperation = "operation"
)

type partialRecord struct {
	Payload opencdc.Data `json:"payload"`
	Key     opencdc.Data `json:"key"`
}

func convertToMessage(record opencdc.Record) *service.Message {
	msg := service.NewMessage(nil)
	var payload opencdc.Data
	if record.Operation == opencdc.OperationDelete {
		// TODO(gregfurman): Should we be capturing data when delete?
		payload = record.Payload.Before
	} else {
		payload = record.Payload.After
	}

	data := partialRecord{
		Payload: payload,
		Key:     record.Key,
	}

	msg.MetaSetMut(metaRecordPosition, record.Position.String())
	msg.MetaSetMut(metaRecordOperation, record.Operation.String())

	for key, value := range record.Metadata {
		fmtKey := strings.ToLower(strings.ReplaceAll(key, ".", "_"))
		msg.MetaSet(fmtKey, value)
	}

	msg.SetStructuredMut(data)

	return msg
}

func convertToRecord(message *service.Message, op opencdc.Operation) (opencdc.Record, error) {
	var meta opencdc.Metadata
	_ = message.MetaWalk(func(key, value string) error {
		if key != metaRecordKey && key != metaRecordPosition && key != metaRecordOperation {
			fmtKey := strings.ReplaceAll(key, "_", ".")
			meta[fmtKey] = value
		}
		return nil
	})

	structured, err := message.AsStructuredMut()
	if err != nil {
		return opencdc.Record{}, err
	}
	data, ok := structured.(partialRecord)
	if !ok {
		return opencdc.Record{}, errInvalidFormat
	}

	switch op {
	case opencdc.OperationCreate:
		return sdk.Util.Source.NewRecordCreate(opencdc.Position(""), meta, data.Key, data.Payload), nil
	case opencdc.OperationUpdate:
		return sdk.Util.Source.NewRecordUpdate(opencdc.Position(""), meta, data.Key, nil, data.Payload), nil
	case opencdc.OperationDelete:
		return sdk.Util.Source.NewRecordDelete(opencdc.Position(""), meta, data.Key, data.Payload), nil
	case opencdc.OperationSnapshot:
		return sdk.Util.Source.NewRecordSnapshot(opencdc.Position(""), meta, data.Key, data.Payload), nil
	default:
		return sdk.Util.Source.NewRecordCreate(opencdc.Position(""), meta, data.Key, data.Payload), nil
	}
}
