package conduit

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit/pkg/conduit"
	"github.com/conduitio/conduit/pkg/foundation/log"
	"github.com/warpstreamlabs/bento/public/service"
)

func init() {
	err := service.RegisterBatchProcessor(
		"conduit", connectorSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newConduitProcessor(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type conduitProcessor struct {
	proc sdk.Processor
	log  *service.Logger
}

func newConduitProcessor(parsedConf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
	conf, err := parseConfig(parsedConf)
	if err != nil {
		return nil, err
	}

	availablePlugins := conduit.DefaultConfig().ProcessorPlugins
	pluginCtor, exists := availablePlugins[conf.plugin]
	if !exists {
		return nil, fmt.Errorf("processor plugin %s not found", conf.plugin)
	}

	proc := pluginCtor(log.Nop())
	if err := proc.Configure(context.Background(), conf.settings); err != nil {
		return nil, fmt.Errorf("failed to configure destination: %w", err)
	}

	return &conduitProcessor{
		proc: proc,
		log:  res.Logger(),
	}, nil
}

func (p *conduitProcessor) ProcessBatch(ctx context.Context, inBatch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(inBatch) == 0 {
		return nil, nil
	}

	outBatch := inBatch.Copy()
	records := make([]opencdc.Record, 0, len(inBatch))
	msgToRecordMap := make(map[int]int)

	recordIndex := 0
	for msgIndex, msg := range outBatch {
		record, err := messageToRecord(msg)
		if err != nil {
			p.log.Debugf("Failed to convert message to record: %v", err)
			msg.SetError(err)
			continue
		}
		records = append(records, record)
		msgToRecordMap[msgIndex] = recordIndex
		recordIndex++
	}

	if len(records) == 0 {
		return []service.MessageBatch{outBatch}, nil
	}

	processedRecords := p.proc.Process(ctx, records)
	if len(processedRecords) != len(records) {
		return nil, fmt.Errorf("processor returned %d records, expected %d", len(processedRecords), len(records))
	}

	var outBatches []service.MessageBatch
	currentBatch := make(service.MessageBatch, 0, len(outBatch))

	for msgIndex, msg := range outBatch {
		if msg.GetError() != nil {
			currentBatch = append(currentBatch, msg)
			continue
		}

		recordIdx, exists := msgToRecordMap[msgIndex]
		if !exists {
			currentBatch = append(currentBatch, msg)
			continue
		}

		switch pr := processedRecords[recordIdx].(type) {
		case sdk.SingleRecord:
			currentBatch = append(currentBatch, recordToMessage(opencdc.Record(pr)))
		case sdk.MultiRecord:
			if len(pr) == 0 {
				continue
			}

			if len(pr) == 1 {
				currentBatch = append(currentBatch, recordToMessage(opencdc.Record(pr[0])))
				continue
			}

			if len(currentBatch) > 0 {
				outBatches = append(outBatches, currentBatch)
				currentBatch = make(service.MessageBatch, 0, len(outBatch))
			}

			multiBatch := make(service.MessageBatch, 0, len(pr))
			for _, record := range pr {
				multiBatch = append(multiBatch, recordToMessage(record))
			}
			outBatches = append(outBatches, multiBatch)

		case sdk.ErrorRecord:
			msg.SetError(pr.Error)
			currentBatch = append(currentBatch, msg)
		case sdk.FilterRecord:
			continue
		default:
			msg.SetError(fmt.Errorf("unknown processed record type: %T", pr))
			currentBatch = append(currentBatch, msg)
		}
	}

	if len(currentBatch) > 0 {
		outBatches = append(outBatches, currentBatch)
	}

	return outBatches, nil
}

func (p *conduitProcessor) Close(context.Context) error {
	return nil
}
