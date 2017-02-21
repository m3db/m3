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

package peers

import (
	"math"
	"runtime"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap/result"
)

var (
	defaultDefaultShardConcurrency        = runtime.NumCPU()
	defaultIncrementalShardConcurrency    = int(math.Max(1, float64(runtime.NumCPU())/2))
	defaultIncrementalPersistMaxQueueSize = 0
)

type options struct {
	resultOpts                     result.Options
	client                         client.AdminClient
	defaultShardConcurrency        int
	incrementalShardConcurrency    int
	incrementalPersistMaxQueueSize int
	persistManager                 persist.Manager
	blockRetrieverManager          block.DatabaseBlockRetrieverManager
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		resultOpts:                     result.NewOptions(),
		defaultShardConcurrency:        defaultDefaultShardConcurrency,
		incrementalShardConcurrency:    defaultIncrementalShardConcurrency,
		incrementalPersistMaxQueueSize: defaultIncrementalPersistMaxQueueSize,
	}
}

func (o *options) SetResultOptions(value result.Options) Options {
	opts := *o
	opts.resultOpts = value
	return &opts
}

func (o *options) ResultOptions() result.Options {
	return o.resultOpts
}

func (o *options) SetAdminClient(value client.AdminClient) Options {
	opts := *o
	opts.client = value
	return &opts
}

func (o *options) AdminClient() client.AdminClient {
	return o.client
}

func (o *options) SetDefaultShardConcurrency(value int) Options {
	opts := *o
	opts.defaultShardConcurrency = value
	return &opts
}

func (o *options) DefaultShardConcurrency() int {
	return o.defaultShardConcurrency
}

func (o *options) SetIncrementalShardConcurrency(value int) Options {
	opts := *o
	opts.incrementalShardConcurrency = value
	return &opts
}

func (o *options) IncrementalShardConcurrency() int {
	return o.incrementalShardConcurrency
}

func (o *options) SetIncrementalPersistMaxQueueSize(value int) Options {
	opts := *o
	opts.incrementalPersistMaxQueueSize = value
	return &opts
}

func (o *options) IncrementalPersistMaxQueueSize() int {
	return o.incrementalPersistMaxQueueSize
}

func (o *options) SetPersistManager(value persist.Manager) Options {
	opts := *o
	opts.persistManager = value
	return &opts
}

func (o *options) PersistManager() persist.Manager {
	return o.persistManager
}

func (o *options) SetDatabaseBlockRetrieverManager(
	value block.DatabaseBlockRetrieverManager,
) Options {
	opts := *o
	opts.blockRetrieverManager = value
	return &opts
}

func (o *options) DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager {
	return o.blockRetrieverManager
}
