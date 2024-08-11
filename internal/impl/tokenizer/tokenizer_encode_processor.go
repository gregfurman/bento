package tokenizer

import (
	"context"
	"errors"

	"github.com/sugarme/tokenizer"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterBatchProcessor(
		"tokenizer_encode", tokenizerEncodeProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newEncoderFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func tokenizerEncodeProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary(`Tokenizes a string input based on a set of pre-defined text encodings.`).
		Categories("Services").
		Fields(tokenizerFields()...)
}

type tokenizerEncoder struct {
	tk     *tokenizer.Tokenizer
	logger *service.Logger
}

func newEncoderFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*tokenizerEncoder, error) {
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

	return &tokenizerEncoder{
		tk: tk,
	}, nil

}

func (te *tokenizerEncoder) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	batch = batch.Copy()
	for _, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}
		input := tokenizer.NewInputSequence(string(msgBytes))

		encoding, err := te.tk.Encode(tokenizer.NewSingleEncodeInput(input), true) //te.tk.EncodeSingle(string(msgBytes))
		if err != nil {
			msg.SetError(err)
			continue
		}

		if encoding == nil || encoding.IsEmpty() {
			msg.SetError(errors.New("encoding is empty"))
			continue
		}
		// te.logger.Infof("%v", encoding)
		msg.SetStructuredMut(encoding)
	}

	return []service.MessageBatch{batch}, nil
}

func (tp *tokenizerEncoder) Close(ctx context.Context) error {
	return nil
}
