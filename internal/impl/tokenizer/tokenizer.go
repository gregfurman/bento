package tokenizer

import (
	"errors"

	"github.com/sugarme/tokenizer"
	"github.com/sugarme/tokenizer/pretrained"
	"github.com/warpstreamlabs/bento/public/service"
)

func tokenizerFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("model_name").
			Description("The model name e.g., \"bert-base-uncase\" or path to directory contains model/config files.").
			Example("bert-base-uncase").
			Example("/path/to/model/files").
			Example("/path/to/config/files").
			Optional(),
		service.NewStringField("config_file").
			Description("Config file name.").
			Default("config.json"),
		service.NewObjectField("pretrained",
			service.NewStringEnumField("tokenizer",
				BertBaseUncased,
				BertLargeCasedWholeWordMaskingSquad,
				GPT2,
				RobertaBase,
				RobertaBaseSquad2,
			).
				Description("Sets the pretrained tokenizer."),
			service.NewBoolField("add_prefix_space").
				Description("If enabled, adds a leading space to the first word, treating the leading word like any other in the sequence.").
				Default(false),
			service.NewBoolField("trim_offsets").
				Description("If enabled, trim offsets in the post-processing step to avoid including whitespaces.").
				Default(true),
		).Optional().Description("Loads a pretrained tokenizer."),
	}
}

const (
	BertBaseUncased                     = "BertBaseUncased"
	BertLargeCasedWholeWordMaskingSquad = "BertLargeCasedWholeWordMaskingSquad"
	GPT2                                = "GPT2"
	RobertaBase                         = "RobertaBase"
	RobertaBaseSquad2                   = "RobertaBaseSquad2"
)

func loadPretrainedTokenizer(tk string, addPrefixSpace bool, trimOffsets bool) (*tokenizer.Tokenizer, error) {
	switch tk {
	case BertBaseUncased:
		return pretrained.BertBaseUncased(), nil
	case BertLargeCasedWholeWordMaskingSquad:
		return pretrained.BertLargeCasedWholeWordMaskingSquad(), nil
	case GPT2:
		return pretrained.GPT2(addPrefixSpace, trimOffsets), nil
	case RobertaBase:
		return pretrained.RobertaBase(addPrefixSpace, trimOffsets), nil
	case RobertaBaseSquad2:
		return pretrained.RobertaBaseSquad2(addPrefixSpace, trimOffsets), nil
	default:
		return nil, errors.New("no pretrained tokenizer found")
	}

}

func loadFromPretrained(conf *service.ParsedConfig) (*tokenizer.Tokenizer, error) {
	tokenizerStr, err := conf.FieldString("pretrained", "tokenizer")
	if err != nil {
		return nil, err
	}

	addPrefixSpace, err := conf.FieldBool("pretrained", "add_prefix_space")
	if err != nil {
		return nil, err
	}

	trimOffsets, err := conf.FieldBool("pretrained", "trim_offsets")
	if err != nil {
		return nil, err
	}

	tk, err := loadPretrainedTokenizer(tokenizerStr, addPrefixSpace, trimOffsets)
	if err != nil {
		return nil, err
	}

	return tk, nil
}

func loadFromConfigFile(conf *service.ParsedConfig) (*tokenizer.Tokenizer, error) {
	modelNameOrPath, err := conf.FieldString("model_name")
	if err != nil {
		return nil, err
	}

	configFile, err := conf.FieldString("config_file")
	if err != nil {
		return nil, err
	}

	configFilePath, err := tokenizer.CachedPath(modelNameOrPath, configFile)
	if err != nil {
		return nil, err
	}

	tk, err := pretrained.FromFile(configFilePath)
	if err != nil {
		return nil, err
	}

	return tk, nil
}

func newTokenizerFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*tokenizer.Tokenizer, error) {
	if conf.Contains("pretrained") {
		return loadFromPretrained(conf)
	}

	return loadFromConfigFile(conf)
}
