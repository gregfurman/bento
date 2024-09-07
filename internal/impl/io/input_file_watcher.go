package io

import (
	"context"
	"log"
	"strconv"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/warpstreamlabs/bento/internal/filepath"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/codec"
)

const (
	fileWatchInputFieldEventsBufferSize = "events_buffer_size"
)

func fileWatcherInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Local").
		Summary(`Monitors one or more paths for changes, emitting notification events where necessary.`).
		Description(`


A path can only be watched once; watching it more than once is a no-op and will not return an error. Paths that do not yet exist on the filesystem cannot be watched.

A watch will be automatically removed if the watched path is deleted or renamed. The exception is the Windows backend, which doesn't remove the watcher on renames.

### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- path
- mod_time_unix
- mod_time (RFC3339)
`+"```"+`

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Example(
			"Read a Bunch of CSVs",
			"If we wished to consume a directory of CSV files as structured documents we can use a glob pattern and the `csv` scanner:",
			`
input:
  file:
    paths: [ ./data/*.csv ]
    scanner:
      csv: {}
`,
		).
		Fields(
			service.NewStringListField(fileInputFieldPaths).
				Description("A list of paths to consume sequentially. Glob patterns are supported, including super globs (double star)."),
			service.NewStringField(fileWatchInputFieldEventsBufferSize).
				Description("The size of the file-system watcher's channel buffer.").
				Advanced().
				Optional(),
		)
}

func init() {
	err := service.RegisterBatchInput("file_watch", fileInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
			r, err := fileConsumerFromParsed(pConf, res)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(pConf, r)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type fileWatcher struct {
	log *service.Logger
	nm  *service.Resources

	paths       []string
	scannerCtor codec.DeprecatedFallbackCodec
	watcher     *fsnotify.Watcher

	scannerMut  sync.Mutex
	scannerInfo *scannerInfo

	watcherChannelSize uint

	delete bool
}

func fileWatcherFromParsed(conf *service.ParsedConfig, nm *service.Resources) (*fileWatcher, error) {
	paths, err := conf.FieldStringList(fileInputFieldPaths)
	if err != nil {
		return nil, err
	}

	expandedPaths, err := filepath.Globs(nm.FS(), paths)
	if err != nil {
		return nil, err
	}

	var chanSize uint64
	if conf.Contains(fileWatchInputFieldEventsBufferSize) {
		bs, err := conf.FieldString(fileWatchInputFieldEventsBufferSize)
		if err != nil {
			return nil, err
		}
		if chanSize, err = strconv.ParseUint(bs, 10, 32); err != nil {
			return nil, err
		}
	}

	return &fileWatcher{
		nm:                 nm,
		log:                nm.Logger(),
		paths:              expandedPaths,
		watcherChannelSize: uint(chanSize),
	}, nil
}

func (f *fileWatcher) Connect(ctx context.Context) error {
	var err error

	if f.watcherChannelSize > 0 {
		f.watcher, err = fsnotify.NewBufferedWatcher(f.watcherChannelSize)
	} else {
		f.watcher, err = fsnotify.NewWatcher()
	}

	if err != nil {
		return err
	}

	for _, path := range f.paths {
		if err := f.watcher.Add(path); err != nil {
			return err
		}
	}

	return nil
}

func (f *fileWatcher) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	type filesystemEvent struct {
		File      string `json:"file"`
		Operation string `json:"operation"`
	}

	fileOperations := map[string]fsnotify.Op{
		fsnotify.Create, fsnotify.Remove, fsnotify.Write, fsnotify.Chmod,
	}

	for {
		select {
		case event, ok := <-f.watcher.Events:
			if !ok {
				return nil, nil, service.ErrEndOfInput
			}

			var batch service.MessageBatch

			for _, op := range fileOperations {
				if event.Op.Has(op) {
					op.String()
					msg := service.NewMessage(nil)
					msg.SetStructuredMut(event)
					batch = append(batch, msg)
				}
			}

			log.Println("event:", event)
			if event.Has(fsnotify.Write) {
				log.Println("modified file:", event.Name)
			}
		case err, ok := <-f.watcher.Errors:
			if !ok {
				return nil, nil, err
			}
		}
	}

	return parts, func(rctx context.Context, res error) error {
		return codecAckFn(rctx, res)
	}, nil

}

func (f *fileWatcher) Close(ctx context.Context) (err error) {
	return f.watcher.Close()
}
