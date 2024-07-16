package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func hugotTokenClassificationConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Machine Learning", "NLP").
		Summary("Performs machine learning inference using a token classification ONNX Runtime model.").
		Description(`Uses Hugot, a library that provides an interface for running ONNX Runtime models and transformer pipelines, with a focus on NLP tasks.

Currently [only implemented](https://github.com/knights-analytics/hugot/tree/main?tab=readme-ov-file#implemented-pipelines):

- [featureExtraction](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.FeatureExtractionPipeline)
- [textClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TextClassificationPipeline)
- [tokenClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TokenClassificationPipeline)`).
		Field(service.NewStringEnumField("aggregation_strategy",
			"SIMPLE",
			"NONE",
		).Description("The aggregation strategy to use for the token classification pipeline.").Default("SIMPLE")).
		Field(service.NewStringListField("ignore_labels").
			Description("Labels to ignore in the token classification pipeline.").
			Default([]string{}).
			Example([]string{"O", "MISC"}))

	for _, f := range hugotConfigSpec() {
		spec = spec.Field(f)
	}

	return spec
}

func init() {
	err := service.RegisterProcessor("token_classifer", hugotTokenClassificationConfigSpec(), newTokenClassificationPipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getTokenClassificationOptions(conf *service.ParsedConfig) ([]pipelines.PipelineOption[*pipelines.TokenClassificationPipeline], error) {
	var options []pipelines.PipelineOption[*pipelines.TokenClassificationPipeline]

	aggregationStrategy, err := conf.FieldString("aggregation_strategy")
	if err != nil {
		return nil, err
	}
	switch aggregationStrategy {
	case "SIMPLE":
		options = append(options, pipelines.WithSimpleAggregation())
	case "NONE":
		options = append(options, pipelines.WithoutAggregation())
	}

	ignoreLabels, err := conf.FieldStringList("ignore_labels")
	if err != nil {
		return nil, err
	}
	if len(ignoreLabels) > 0 {
		options = append(options, pipelines.WithIgnoreLabels(ignoreLabels))
	}

	return options, nil
}

//------------------------------------------------------------------------------

func newTokenClassificationPipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getTokenClassificationOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.TokenClassificationConfig{
		Name:         p.name,
		OnnxFilename: p.onnxFilename,
		ModelPath:    p.modelPath,
		Options:      opts,
	}

	if p.pipeline, err = hugot.NewPipeline(p.session, cfg); err != nil {
		return nil, err
	}

	return p, nil
}
