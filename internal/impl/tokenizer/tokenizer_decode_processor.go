package tokenizer

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/sugarme/tokenizer"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterBatchProcessor(
		"tokenizer_decode", tokenizerDecoderProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newDecoderFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func tokenizerDecoderProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary(`Decodes tokenized string input based on a set of pre-defined text encodings.`).
		Categories("Services").
		Fields(tokenizerFields()...)
}

type tokenizerDecoder struct {
	tk     *tokenizer.Tokenizer
	logger *service.Logger
}

func newDecoderFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*tokenizerDecoder, error) {
	var (
		tk  *tokenizer.Tokenizer
		err error
	)

	if conf.Contains("pretrained") {
		if tk, err = loadFromPretrained(conf); err != nil {
			return nil, err
		}
	} else {
		if tk, err = loadFromConfigFile(conf); err != nil {
			return nil, err
		}
	}

	return &tokenizerDecoder{
		tk:     tk,
		logger: mgr.Logger(),
	}, nil

}

func (td *tokenizerDecoder) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	type EncodingIds struct {
		Ids []int `json:"Ids"`
	}

	batch = batch.Copy()
	for _, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}

		var encoding EncodingIds
		if err := json.Unmarshal(msgBytes, &encoding); err != nil {
			msg.SetError(err)
			continue
		}

		if len(encoding.Ids) == 0 || encoding.Ids == nil {
			msg.SetError(errors.New("encoding is empty"))
			continue
		}

		td.logger.Infof("%v", td.tk.GetModel())
		td.logger.Infof("%v", td.tk.GetDecoder())

		decodedTokens := td.tk.Decode(encoding.Ids, true)
		msg.SetStructuredMut(decodedTokens)
	}

	return []service.MessageBatch{batch}, nil
}

func (td *tokenizerDecoder) Close(ctx context.Context) error {
	return nil
}
