package huggingface

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"

	"github.com/warpstreamlabs/bento/public/service"
)

func hugotConfigSpec() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("pipeline_name").
			Description("Name of the pipeline."),
		service.NewStringField("model_name").
			Description("Name of the huggingface model.").
			Example("KnightsAnalytics/distilbert-base-uncased-finetuned-sst-2-english").
			Example("google-bert/bert-base-uncased"),
		service.NewStringField("model_path").
			Description("Local path for the ONNX model file. If specified and the file doesn't exist, the model will be downloaded to this location.").
			Example("/path/to/models/my_model.onnx"),
		service.NewStringField("onnx_library_path").
			Description("The location of the ONNX Runtime dynamic library.").
			Default("/usr/lib/libonnxruntime.so").
			Advanced(),
		service.NewStringField("onnx_filename").
			Description("The filename of the model to run. Only necessary to specify when multiple .onnx files are present.").
			Example("model.onnx").
			Default("").
			Advanced(),
	}
}

//------------------------------------------------------------------------------

type pipelineProcessor struct {
	log *service.Logger

	session  *hugot.Session
	pipeline pipelines.Pipeline

	modelPath    string
	name         string
	onnxFilename string

	closeOnce sync.Once
}

func newPipelineProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*pipelineProcessor, error) {
	p := &pipelineProcessor{log: mgr.Logger()}

	var modelPath, pipelineName string
	var onnxLibraryPath, onnxFileName string

	var err error

	if modelPath, err = conf.FieldString("model_path"); err != nil {
		return nil, err
	}

	if pipelineName, err = conf.FieldString("pipeline_name"); err != nil {
		return nil, err
	}

	if onnxFileName, err = conf.FieldString("onnx_filename"); err != nil {
		return nil, err
	}

	if onnxLibraryPath, err = conf.FieldString("onnx_library_path"); err != nil {
		return nil, err
	}

	if p.session, err = globalSession.NewSession(onnxLibraryPath); err != nil {
		return nil, err
	}

	p.onnxFilename = onnxFileName
	p.modelPath = modelPath
	p.name = pipelineName

	return p, nil
}

//------------------------------------------------------------------------------

func (p *pipelineProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	var msgContents []string
	if err := json.Unmarshal(msgBytes, &msgContents); err != nil {
		return nil, err
	}

	results, err := p.pipeline.Run(msgContents)
	if err != nil {
		return nil, err
	}

	msg.SetStructuredMut(results.GetOutput())

	return service.MessageBatch{msg}, nil
}

func (p *pipelineProcessor) Close(context.Context) error {
	p.closeOnce.Do(func() {
		globalSession.Destroy()
	})
	return nil
}
