package conduit

import (
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	metaRecordPosition = "position"
	metaRecordKey      = "key"
)

func convertToMessage(record opencdc.Record) *service.Message {
	msg := service.NewMessage(nil)

	var payload opencdc.Data
	if record.Operation == opencdc.OperationDelete {
		// TODO(gregfurman): Should we be capturing data when delete?
		payload = record.Payload.Before
	} else {
		payload = record.Payload.After
	}

	switch p := payload.(type) {
	case opencdc.StructuredData:
		msg.SetStructured(p)
	case opencdc.RawData:
		msg.SetBytes(p)
	}

	switch key := record.Key.(type) {
	case opencdc.StructuredData:
		msg.MetaSetMut(metaRecordKey, key)
	case opencdc.RawData:
		msg.MetaSetMut(metaRecordKey, key)
	}

	msg.MetaSetMut(metaRecordPosition, record.Position.String())

	for key, value := range record.Metadata {
		fmtKey := strings.ToLower(strings.ReplaceAll(key, ".", "_"))
		msg.MetaSet(fmtKey, value)
	}

	return msg
}

func convertToInsertRecord(message *service.Message) opencdc.Record {
	// TODO(gregfurman): Try and properly map all metadata from message to
	// opencdc.Metadata keys
	var meta opencdc.Metadata
	_ = message.MetaWalk(func(key, value string) error {
		fmtKey := strings.ReplaceAll(key, "_", ".")
		meta[fmtKey] = value
	})

	message.AsStructuredMut()
	record := sdk.Util.Source.NewRecordCreate(
		opencdc.Position(""),
		meta,
		opencdc.RawData(nil),
		opencdc.RawData(messageBody),
	)

	return msg
}
