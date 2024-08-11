package triton

import (
	"context"

	"github.com/warpstreamlabs/bento/public/service"
)

type tritonProcessor struct {
	client *tritonClient
}

func newTritonProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*tritonProcessor, error) {
	urlStr, err := conf.FieldString("url")
	if err != nil {
		return nil, err
	}

	client, err := newTritonInferenceClient(urlStr)
	if err != nil {
		return nil, err
	}

	isLive, err := client.ServerLiveRequest()
	if !isLive.GetLive() {
		return nil, service.ErrNotConnected
	}
	mgr.Logger().Debugf("Triton Health - Live: %v\n", isLive.Live)

	isReady, err := client.ServerReadyRequest()
	if !isReady.GetReady() {
		return nil, service.ErrNotConnected
	}
	mgr.Logger().Debugf("Triton Health - Ready: %v\n", isReady.Ready)

	return &tritonProcessor{
		client: client,
	}, nil
}

func (tp *tritonProcessor) ProcessBatch(ctx context.Context, inBatch service.MessageBatch) ([]service.MessageBatch, error) {

	newMsg := inBatch.Copy()
	// for index, part := range newMsg {
	// 	model, ok := part.MetaGet("triton_model")
	// 	if !ok {
	// 		continue
	// 	}

	// 	Preprocess()

	// 	tp.client.ModelInferRequest(nil, model)
	// }
	return []service.MessageBatch{newMsg}, nil
}

func (tp *tritonProcessor) Close(ctx context.Context) error {
	return tp.client.Close()
}
