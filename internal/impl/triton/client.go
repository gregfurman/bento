package triton

import (
	"bytes"
	"context"
	"encoding/binary"
	"time"

	triton "github.com/warpstreamlabs/bento/internal/impl/triton/resources/grpc-client"
	"github.com/warpstreamlabs/bento/public/service"
	"google.golang.org/grpc"
)

func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewURLField("url").
			Description("Inference Server URL.").
			Example("localhost:8001").
			Default("localhost:8001"),
		service.NewStringEnumField("kind", "simple", "cluster", "failover").
			Description("Specifies a simple, cluster-aware, or failover-aware redis client.").
			Default("simple").
			Advanced(),
		service.NewStringField("model").
			Description("Name of model being served.").
			Default("").
			Example("inception_graphdef").
			Example("densenet_onnx").
			Advanced(),
		service.NewStringField("version").
			Description("Version of model.").
			Default("").
			Example("mymaster").
			Advanced(),
	}
}

func (tc *tritonClient) ServerLiveRequest() (*triton.ServerLiveResponse, error) {
	// Create context for our request with 10 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serverLiveRequest := triton.ServerLiveRequest{}
	// Submit ServerLive request to server
	serverLiveResponse, err := tc.inferenceClient.ServerLive(ctx, &serverLiveRequest)
	if err != nil {
		return nil, err
		// log.Fatalf("Couldn't get server live: %v", err)
	}
	return serverLiveResponse, nil
}

func (tc *tritonClient) ServerReadyRequest() (*triton.ServerReadyResponse, error) {
	// Create context for our request with 10 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	serverReadyRequest := triton.ServerReadyRequest{}
	// Submit ServerReady request to server
	serverReadyResponse, err := tc.inferenceClient.ServerReady(ctx, &serverReadyRequest)
	if err != nil {
		return nil, err
		// log.Fatalf("Couldn't get server ready: %v", err)
	}
	return serverReadyResponse, nil
}

func (tc *tritonClient) ModelMetadataRequest(modelName string, modelVersion string) (*triton.ModelMetadataResponse, error) {
	// Create context for our request with 10 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create status request for a given model
	modelMetadataRequest := triton.ModelMetadataRequest{
		Name:    modelName,
		Version: modelVersion,
	}

	// Submit modelMetadata request to server
	modelMetadataResponse, err := tc.inferenceClient.ModelMetadata(ctx, &modelMetadataRequest)
	if err != nil {
		return nil, err
		// log.Fatalf("Couldn't get server model metadata: %v", err)
	}
	return modelMetadataResponse, nil
}

func (tc *tritonClient) ModelConfigRequest(modelName string, modelVersion string) (*triton.ModelConfigResponse, error) {
	// Create context for our request with 10 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create status request for a given model
	modelMetadataRequest := triton.ModelConfigRequest{
		Name:    modelName,
		Version: modelVersion,
	}

	// Submit modelMetadata request to server
	modelConfigResponse, err := tc.inferenceClient.ModelConfig(ctx, &modelMetadataRequest)
	if err != nil {
		return nil, err
		// log.Fatalf("Couldn't get server model metadata: %v", err)
	}
	return modelConfigResponse, nil
}

func (tc *tritonClient) ModelInferRequest(rawInput [][]byte, modelName string, modelVersion string) (*triton.ModelInferResponse, error) {
	// Create context for our request with 10 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := tc.ModelConfigRequest(modelName, modelVersion)
	if err != nil {
		return nil, err
	}

	inferInputs := make([]*triton.ModelInferRequest_InferInputTensor, len(resp.Config.Input))
	for i, input := range resp.Config.Input {
		inferInputs[i] = &triton.ModelInferRequest_InferInputTensor{
			Name:     input.GetName(),
			Datatype: input.GetDataType().String(),
			Shape:    input.GetDims(),
		}
	}

	// Create request input output tensors
	inferOutputs := make([]*triton.ModelInferRequest_InferRequestedOutputTensor, len(resp.Config.Output))
	for i, output := range resp.Config.Output {
		inferOutputs[i] = &triton.ModelInferRequest_InferRequestedOutputTensor{
			Name: output.GetName(),
		}
	}
	// Create inference request for specific model/version
	modelInferRequest := triton.ModelInferRequest{
		ModelName:        modelName,
		ModelVersion:     modelVersion,
		Inputs:           inferInputs,
		Outputs:          inferOutputs,
		RawInputContents: rawInput,
	}

	// Submit inference request to server
	modelInferResponse, err := tc.inferenceClient.ModelInfer(ctx, &modelInferRequest)
	if err != nil {
		return nil, err
		// log.Fatalf("Error processing InferRequest: %v", err)
	}
	return modelInferResponse, nil
}

// Convert int32 input data into raw bytes (assumes Little Endian)
func Preprocess(inputs [][]int32) [][]byte {
	result := make([][]byte, len(inputs))

	for i, input := range inputs {
		inputBytes := make([]byte, 0, len(input)*4) // Pre-allocate capacity
		bs := make([]byte, 4)

		for _, value := range input {
			binary.LittleEndian.PutUint32(bs, uint32(value))
			inputBytes = append(inputBytes, bs...)
		}

		result[i] = inputBytes
	}

	return result
}

// Convert slice of 4 bytes to int32 (assumes Little Endian)
func readInt32(fourBytes []byte) int32 {
	buf := bytes.NewBuffer(fourBytes)
	var retval int32
	binary.Read(buf, binary.LittleEndian, &retval)
	return retval
}

// Postprocess converts raw byte slices from the inference response into slices of the specified data type
func Postprocess(inferResponse *triton.ModelInferResponse) ([]interface{}, error) {
	outputCount := len(inferResponse.RawOutputContents)
	result := make([]interface{}, outputCount)

	for i, outputBytes := range inferResponse.RawOutputContents {
		// switch dataType {
		// case reflect.Int32:
		data := make([]int32, len(outputBytes)/4)
		for j := 0; j < len(data); j++ {
			data[j] = int32(binary.LittleEndian.Uint32(outputBytes[j*4 : (j+1)*4]))
		}
		result[i] = data
		// case reflect.Float32:
		// 	data := make([]float32, len(outputBytes)/4)
		// 	for j := 0; j < len(data); j++ {
		// 		data[j] = math.Float32frombits(binary.LittleEndian.Uint32(outputBytes[j*4 : (j+1)*4]))
		// 	}
		// 	result[i] = data
		// // Add more cases for other data types as needed
		// default:
		// 	return nil, fmt.Errorf("unsupported data type: %v", dataType)
		// }
	}

	return result, nil
}

type tritonClient struct {
	inferenceClient triton.GRPCInferenceServiceClient
	conn            *grpc.ClientConn
}

func newTritonInferenceClient(url string) (*tritonClient, error) {
	conn, err := grpc.NewClient(url)
	if err != nil {
		return nil, err
	}

	client := triton.NewGRPCInferenceServiceClient(conn)

	return &tritonClient{
		inferenceClient: client,
		conn:            conn,
	}, nil
}

func (tc *tritonClient) Close() error {
	return tc.conn.Close()
}

// func (tc *tritonClient) getDimensions()

// func main() {
// 	// Create client from gRPC server connection
// 	client := triton.NewGRPCInferenceServiceClient(conn)

// 	serverLiveResponse := ServerLiveRequest(client)
// 	fmt.Printf("Triton Health - Live: %v\n", serverLiveResponse.Live)

// 	serverReadyResponse := ServerReadyRequest(client)
// 	fmt.Printf("Triton Health - Ready: %v\n", serverReadyResponse.Ready)

// 	modelMetadataResponse := ModelMetadataRequest(client, FLAGS.ModelName, "")
// 	fmt.Println(modelMetadataResponse)

// 	inputData0 := make([]int32, inputSize)
// 	inputData1 := make([]int32, inputSize)
// 	for i := 0; i < inputSize; i++ {
// 		inputData0[i] = int32(i)
// 		inputData1[i] = 1
// 	}
// 	inputs := [][]int32{inputData0, inputData1}
// 	rawInput := Preprocess(inputs)

// 	/* We use a simple model that takes 2 input tensors of 16 integers
// 	each and returns 2 output tensors of 16 integers each. One
// 	output tensor is the element-wise sum of the inputs and one
// 	output is the element-wise difference. */
// 	inferResponse := ModelInferRequest(client, rawInput, FLAGS.ModelName, FLAGS.ModelVersion)

// 	/* We expect there to be 2 results (each with batch-size 1). Walk
// 	over all 16 result elements and print the sum and difference
// 	calculated by the model. */
// 	outputs := Postprocess(inferResponse)
// 	outputData0 := outputs[0]
// 	outputData1 := outputs[1]

// 	fmt.Println("\nChecking Inference Outputs\n--------------------------")
// 	for i := 0; i < outputSize; i++ {
// 		fmt.Printf("%d + %d = %d\n", inputData0[i], inputData1[i], outputData0[i])
// 		fmt.Printf("%d - %d = %d\n", inputData0[i], inputData1[i], outputData1[i])
// 		if (inputData0[i]+inputData1[i] != outputData0[i]) ||
// 			inputData0[i]-inputData1[i] != outputData1[i] {
// 			log.Fatalf("Incorrect results from inference")
// 		}
// 	}
// }
