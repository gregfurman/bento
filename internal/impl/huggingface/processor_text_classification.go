//go:build huggingbento

package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"
	"github.com/knights-analytics/tokenizers"

	"github.com/warpstreamlabs/bento/public/service"
)

func hugotTextClassificationConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Machine Learning", "NLP").
		Summary("Performs machine learning inference using a text classification ONNX Runtime model.").
		Description(`Uses Hugot, a library that provides an interface for running ONNX Runtime models and transformer pipelines, with a focus on NLP tasks.

Currently [only implemented](https://github.com/knights-analytics/hugot/tree/main?tab=readme-ov-file#implemented-pipelines):

- [featureExtraction](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.FeatureExtractionPipeline)
- [textClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TextClassificationPipeline)
- [tokenClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TokenClassificationPipeline)`).
		Field(service.NewStringEnumField("aggregation_function",
			"SOFTMAX",
			"SIGMOID",
		).Description("The aggregation function to use for the text classification pipeline.").Default("SOFTMAX")).
		Field(service.NewStringEnumField("problem_type",
			"singleLabel",
			"multiLabel",
		).Description("The problem type for the text classification pipeline.").Default("singleLabel"))

	for _, f := range hugotConfigSpec() {
		spec = spec.Field(f)
	}

	return spec
}

func init() {
	err := service.RegisterBatchProcessor("huggingface_text_classifer", hugotTextClassificationConfigSpec(), newTextClassificationPipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getTextClassificationOptions(conf *service.ParsedConfig) ([]pipelines.PipelineOption[*pipelines.TextClassificationPipeline], error) {
	var options []pipelines.PipelineOption[*pipelines.TextClassificationPipeline]

	aggregationFunction, err := conf.FieldString("aggregation_function")
	if err != nil {
		return nil, err
	}
	switch aggregationFunction {
	case "SOFTMAX":
		options = append(options, pipelines.WithSoftmax())
	case "SIGMOID":
		options = append(options, pipelines.WithSigmoid())
	}

	problemType, err := conf.FieldString("problem_type")
	if err != nil {
		return nil, err
	}
	switch problemType {
	case "singleLabel":
		options = append(options, pipelines.WithSingleLabel())
	case "multiLabel":
		options = append(options, pipelines.WithMultiLabel())
	}

	return options, nil
}

//------------------------------------------------------------------------------

func newTextClassificationPipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getTextClassificationOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.TextClassificationConfig{
		Name:         p.name,
		OnnxFilename: p.onnxFilename,
		ModelPath:    p.modelPath,
		Options:      opts,
	}

	pipeline, err := hugot.NewPipeline(p.session, cfg)
	if err != nil {
		return nil, err
	}

	pipeline.TokenizerOptions = []tokenizers.EncodeOption{
		tokenizers.WithReturnAttentionMask(),
		tokenizers.WithReturnTypeIDs(),
	}

	p.pipeline = pipeline

	return p, nil
}
