package conduit

import (
	"encoding/json"
	"errors"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/warpstreamlabs/bento/public/service"
)

var errInvalidFormat = errors.New("cannot convert message to opencdc format")

const (
	metaRecordPosition  = "conduit_position"
	metaRecordOperation = "conduit_operation"
)

func recordToMessage(record opencdc.Record) *service.Message {
	msg := service.NewMessage(nil)
	mRecord := record.Map()

	data := map[string]interface{}{}

	if key := mRecord["key"]; key != nil {
		data["key"] = asStructured(key)
	}

	if payload := mRecord["payload"]; payload != nil {
		data["payload"] = asStructured(payload)
	}

	msg.MetaSetMut(metaRecordPosition, record.Position.String())
	msg.MetaSetMut(metaRecordOperation, record.Operation.String())

	for key, value := range record.Metadata {
		msg.MetaSet(key, value)
	}

	msg.SetStructuredMut(data)

	return msg
}

func asStructured(data interface{}) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		for k, val := range v {
			v[k] = asStructured(val)
		}
		return v
	case []byte:
		var parsed interface{}
		if err := json.Unmarshal(v, &parsed); err == nil {
			return parsed
		}
		return map[string]interface{}{}
	default:
		return v
	}
}

func messageToRecord(message *service.Message) (opencdc.Record, error) {
	record := opencdc.Record{}
	bMsg, err := message.AsBytes()
	if err != nil {
		return opencdc.Record{}, err
	}

	if err := record.UnmarshalJSON(bMsg); err != nil {
		return opencdc.Record{}, err
	}

	if record.Metadata == nil {
		record.Metadata = make(opencdc.Metadata)
	}

	_ = message.MetaWalk(func(key, value string) error {
		if _, ok := record.Metadata[key]; ok {
			return nil
		}

		if key != metaRecordPosition && key != metaRecordOperation {
			record.Metadata[key] = value
		}

		return nil
	})

	if record.Operation == 0 {
		if op, exists := message.MetaGet(metaRecordOperation); exists {
			record.Operation.UnmarshalText([]byte(op))
		}
	}

	if record.Position == nil {
		if pos, exists := message.MetaGet(metaRecordPosition); exists {
			record.Position = opencdc.Position(pos)
		}
	}

	return record, nil
}
