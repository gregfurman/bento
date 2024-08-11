package triton

import (
	"fmt"
	"time"

	"github.com/sunhailin-Leo/triton-service-go/v2/models/transformers"
	"github.com/sunhailin-Leo/triton-service-go/v2/nvidia_inferenceserver"
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	tBertModelSegmentIdsKey                       string = "segment_ids"
	tBertModelSegmentIdsDataType                  string = "INT32"
	tBertModelInputIdsKey                         string = "input_ids"
	tBertModelInputIdsDataType                    string = "INT32"
	tBertModelInputMaskKey                        string = "input_mask"
	tBertModelInputMaskDataType                   string = "INT32"
	tBertModelOutputProbabilitiesKey              string = "probability"
	tBertModelRespBodyOutputBinaryDataKey         string = "binary_data"
	tBertModelRespBodyOutputClassificationDataKey string = "classification"
)

// testGenerateModelInferRequest Triton Input
func testGenerateModelInferRequest() []*nvidia_inferenceserver.ModelInferRequest_InferInputTensor {
	return []*nvidia_inferenceserver.ModelInferRequest_InferInputTensor{
		{
			Name:     tBertModelSegmentIdsKey,
			Datatype: tBertModelSegmentIdsDataType,
		},
		{
			Name:     tBertModelInputIdsKey,
			Datatype: tBertModelInputIdsDataType,
		},
		{
			Name:     tBertModelInputMaskKey,
			Datatype: tBertModelInputMaskDataType,
		},
	}
}

// testGenerateModelInferOutputRequest Triton Output
func testGenerateModelInferOutputRequest(params ...interface{}) []*nvidia_inferenceserver.ModelInferRequest_InferRequestedOutputTensor {
	return []*nvidia_inferenceserver.ModelInferRequest_InferRequestedOutputTensor{
		{
			Name: tBertModelOutputProbabilitiesKey,
			Parameters: map[string]*nvidia_inferenceserver.InferParameter{
				tBertModelRespBodyOutputBinaryDataKey: {
					ParameterChoice: &nvidia_inferenceserver.InferParameter_BoolParam{BoolParam: false},
				},
				tBertModelRespBodyOutputClassificationDataKey: {
					ParameterChoice: &nvidia_inferenceserver.InferParameter_Int64Param{Int64Param: 1},
				},
			},
		},
	}
}

// testModerInferCallback infer call back (process model infer data)
func testModerInferCallback(inferResponse interface{}, params ...interface{}) ([]interface{}, error) {
	fmt.Println(inferResponse)
	fmt.Println(params...)
	return nil, nil
}

func main() {
	vocabPath := "<Your Bert Vocab Path>"
	maxSeqLen := 48
	httpAddr := "<HTTP URL>"
	grpcAddr := "<GRPC URL>"
	defaultHttpClient := &fasthttp.Client{}
	defaultGRPCClient, grpcErr := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if grpcErr != nil {
		panic(grpcErr)
	}

	// Service
	bertService, initErr := transformers.NewBertModelService(
		vocabPath, httpAddr, defaultHttpClient, defaultGRPCClient,
		testGenerateModelInferRequest, testGenerateModelInferOutputRequest, testModerInferCallback)
	if initErr != nil {
		panic(initErr)
	}
	bertService.SetChineseTokenize(false).SetMaxSeqLength(maxSeqLen)
	// infer
	inferResultV1, inferErr := bertService.ModelInfer([]string{"<Data>"}, "<Model Name>", "<Model Version>", 1*time.Second)
	if inferErr != nil {
		panic(inferErr)
	}
	println(inferResultV1)
}
