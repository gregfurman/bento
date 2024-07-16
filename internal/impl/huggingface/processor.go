package huggingface

import (
	"context"
	"encoding/json"
	"path"
	"strings"
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
		(service.NewStringField("model_path").
			Description("Local path for the ONNX model file. If specified and the file doesn't exist, the model will be downloaded to this location.").
			Example("/path/to/models/my_model.onnx")),
		service.NewObjectField("model_download_options",
			service.NewBoolField("enabled").
				Description("When true, attempts to download a model if it does not exist locally.").
				Default(true),
			service.NewStringField("auth_token").
				Description("Authentication token for accessing private repositories or models.").
				Default(""),
			service.NewBoolField("skip_sha").
				Description("If true, skips SHA verification of downloaded files.").
				Default(false),
			service.NewStringField("branch").
				Description("Specifies the branch to download from in the model repository.").
				Default("main"),
			service.NewIntField("max_retries").
				Description("Maximum number of retry attempts for failed downloads.").
				Default(5),
			service.NewIntField("retry_interval").
				Description("Time interval between retry attempts.").
				Default(5),
			service.NewIntField("concurrent_connections").
				Description("Number of concurrent connections to use during download.").
				Default(5),
			service.NewBoolField("enable_verbose").
				Description("If true, enables verbose logging during the download process.").
				Default(false),
		).Description("Options for configuring model downloads.").Optional().Advanced(),
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

func parseDownloadOptions(conf *service.ParsedConfig) (opts hugot.DownloadOptions, err error) {

	opts = hugot.NewDownloadOptions()

	if opts.AuthToken, err = conf.FieldString("model_download_options", "auth_token"); err != nil {
		return
	}

	if opts.SkipSha, err = conf.FieldBool("model_download_options", "skip_sha"); err != nil {
		return
	}

	if opts.Branch, err = conf.FieldString("model_download_options", "branch"); err != nil {
		return
	}

	if opts.MaxRetries, err = conf.FieldInt("model_download_options", "max_retries"); err != nil {
		return
	}

	if opts.RetryInterval, err = conf.FieldInt("model_download_options", "retry_interval"); err != nil {
		return
	}

	if opts.ConcurrentConnections, err = conf.FieldInt("model_download_options", "concurrent_connections"); err != nil {
		return
	}

	if opts.Verbose, err = conf.FieldBool("model_download_options", "enable_verbose"); err != nil {
		return
	}

	return

}

func newPipelineProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*pipelineProcessor, error) {
	p := &pipelineProcessor{log: mgr.Logger()}

	var modelName, modelPath, pipelineName string
	var onnxLibraryPath, onnxFileName string

	var err error

	if modelName, err = conf.FieldString("model_name"); err != nil {
		return nil, err
	}

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

	var enableDownload bool
	if enableDownload, err = conf.FieldBool("model_download_options", "enabled"); err != nil {
		return nil, err
	}

	var modelDownloadOptions hugot.DownloadOptions
	if modelDownloadOptions, err = parseDownloadOptions(conf); err != nil {
		return nil, err
	}

	if p.session, err = hugot.NewSession(hugot.WithOnnxLibraryPath(onnxLibraryPath)); err != nil {
		return nil, err
	}

	if enableDownload {
		if modelPath, err = p.session.DownloadModel(modelName, modelPath, modelDownloadOptions); err != nil {
			return nil, err
		}
	} else {
		modelPath = constructModelPath(modelName, modelPath)
	}

	p.onnxFilename = onnxFileName
	p.modelPath = modelPath
	p.name = pipelineName

	return p, nil
}

func constructModelPath(modelName, destination string) string {
	// replicates code in hf downloader
	modelP := modelName
	if strings.Contains(modelP, ":") {
		modelP = strings.Split(modelName, ":")[0]
	}
	return path.Join(destination, strings.Replace(modelP, "/", "_", -1))
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
		p.session.Destroy()
	})
	return nil
}
