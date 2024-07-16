package huggingface

import (
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func hugotFeatureExtractionConfigSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Machine Learning", "NLP").
		Summary("Performs machine learning inference using a feature extraction ONNX Runtime model.").
		Description(`Uses Hugot, a library that provides an interface for running ONNX Runtime models and transformer pipelines, with a focus on NLP tasks.

Currently [only implemented](https://github.com/knights-analytics/hugot/tree/main?tab=readme-ov-file#implemented-pipelines):

- [featureExtraction](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.FeatureExtractionPipeline)
- [textClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TextClassificationPipeline)
- [tokenClassification](https://huggingface.co/docs/transformers/en/main_classes/pipelines#transformers.TokenClassificationPipeline)`).
		Field(service.NewBoolField("normalization").
			Description("Whether to apply normalization in the feature extraction pipeline.").
			Default(false))

	for _, f := range hugotConfigSpec() {
		spec = spec.Field(f)
	}

	return spec
}

func init() {
	err := service.RegisterProcessor("feature_extractor", hugotFeatureExtractionConfigSpec(), newFeatureExtractionipeline)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func getFeatureExtractionOptions(conf *service.ParsedConfig) ([]pipelines.PipelineOption[*pipelines.FeatureExtractionPipeline], error) {
	var options []pipelines.PipelineOption[*pipelines.FeatureExtractionPipeline]

	normalization, err := conf.FieldBool("normalization")
	if err != nil {
		return nil, err
	}
	if normalization {
		options = append(options, pipelines.WithNormalization())
	}

	return options, nil
}

//------------------------------------------------------------------------------

func newFeatureExtractionipeline(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	p, err := newPipelineProcessor(conf, mgr)
	if err != nil {
		return nil, err
	}

	opts, err := getFeatureExtractionOptions(conf)
	if err != nil {
		return nil, err
	}

	cfg := hugot.FeatureExtractionConfig{
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
