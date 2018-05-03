// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package bootstrap

import (
	"sync"

	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

// bootstrapProcessProvider is the bootstrapping process provider.
type bootstrapProcessProvider struct {
	sync.RWMutex
	opts                 result.Options
	log                  xlog.Logger
	bootstrapperProvider BootstrapperProvider
}

// NewProcessProvider creates a new bootstrap process provider.
func NewProcessProvider(
	bootstrapperProvider BootstrapperProvider,
	opts result.Options,
) ProcessProvider {
	return &bootstrapProcessProvider{
		opts:                 opts,
		log:                  opts.InstrumentOptions().Logger(),
		bootstrapperProvider: bootstrapperProvider,
	}
}

func (b *bootstrapProcessProvider) SetBootstrapperProvider(bootstrapperProvider BootstrapperProvider) {
	b.Lock()
	defer b.Unlock()
	b.bootstrapperProvider = bootstrapperProvider
}

func (b *bootstrapProcessProvider) BootstrapperProvider() BootstrapperProvider {
	b.RLock()
	defer b.RUnlock()
	return b.bootstrapperProvider
}

func (b *bootstrapProcessProvider) Provide() Process {
	b.RLock()
	defer b.RUnlock()
	return bootstrapProcess{
		opts:         b.opts,
		log:          b.log,
		bootstrapper: b.bootstrapperProvider.Provide(),
	}
}

type bootstrapProcess struct {
	opts         result.Options
	log          xlog.Logger
	bootstrapper Bootstrapper
}

func (b bootstrapProcess) Run(
	nsMetadata namespace.Metadata,
	shards []uint32,
	targetRanges []TargetRange,
) (result.DataBootstrapResult, result.IndexBootstrapResult, error) {
	namespace := nsMetadata.ID()
	bootstrapResult := result.NewDataBootstrapResult()
	for _, target := range targetRanges {
		shardsTimeRanges := make(result.ShardTimeRanges, len(shards))

		window := target.Range

		r := xtime.Ranges{}.AddRange(window)
		for _, s := range shards {
			shardsTimeRanges[s] = r
		}

		logFields := []xlog.Field{
			xlog.NewField("bootstrapper", b.bootstrapper.String()),
			xlog.NewField("namespace", namespace.String()),
			xlog.NewField("numShards", len(shards)),
			xlog.NewField("from", window.Start.String()),
			xlog.NewField("to", window.End.String()),
			xlog.NewField("range", window.End.Sub(window.Start).String()),
		}
		b.log.WithFields(logFields...).Infof("bootstrapping shards for range starting")

		nowFn := b.opts.ClockOptions().NowFn()
		begin := nowFn()

		opts := target.RunOptions
		if opts == nil {
			opts = NewRunOptions()
		}

		res, err := b.bootstrapper.BootstrapData(nsMetadata, shardsTimeRanges, opts)

		logFields = append(logFields, xlog.NewField("took", nowFn().Sub(begin).String()))
		if err != nil {
			logFields = append(logFields, xlog.NewField("error", err.Error()))
			b.log.WithFields(logFields...).Infof("bootstrapping shards for range completed with error")
			return nil, nil, err
		}

		b.log.WithFields(logFields...).Infof("bootstrapping shards for range completed successfully")
		bootstrapResult = result.MergedBootstrapResult(bootstrapResult, res)
	}

	return bootstrapResult, nil, nil
}
