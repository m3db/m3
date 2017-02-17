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
	"runtime"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap/result"
)

var (
	defaultDefaultBootstrapShardConcurrency    = runtime.NumCPU()
	defaultIncrementalShardConcurrency         = 4
	defaultIncrementalBootstrapPersistMaxQueue = 1
)

type options struct {
	resultOpts                           result.Options
	client                               client.AdminClient
	defaultBootstrapShardConcurrency     int
	incrementalBootstrapShardConcurrency int
	incrementalBootstrapPersistMaxQueue  int
	newPersistManagerFn                  storage.NewPersistManagerFn
	blockRetrieverManager                block.DatabaseBlockRetrieverManager
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		resultOpts:                           result.NewOptions(),
		defaultBootstrapShardConcurrency:     defaultDefaultBootstrapShardConcurrency,
		incrementalBootstrapShardConcurrency: defaultIncrementalShardConcurrency,
		incrementalBootstrapPersistMaxQueue:  defaultIncrementalBootstrapPersistMaxQueue,
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

func (o *options) SetDefaultBootstrapShardConcurrency(value int) Options {
	opts := *o
	opts.defaultBootstrapShardConcurrency = value
	return &opts
}

func (o *options) DefaultBootstrapShardConcurrency() int {
	return o.defaultBootstrapShardConcurrency
}

func (o *options) SetIncrementalBootstrapShardConcurrency(value int) Options {
	opts := *o
	opts.incrementalBootstrapShardConcurrency = value
	return &opts
}

func (o *options) IncrementalBootstrapShardConcurrency() int {
	return o.incrementalBootstrapShardConcurrency
}

func (o *options) SetIncrementalBootstrapPersistMaxQueue(value int) Options {
	opts := *o
	opts.incrementalBootstrapPersistMaxQueue = value
	return &opts
}

func (o *options) IncrementalBootstrapPersistMaxQueue() int {
	return o.incrementalBootstrapPersistMaxQueue
}

func (o *options) SetNewPersistManagerFn(value storage.NewPersistManagerFn) Options {
	opts := *o
	opts.newPersistManagerFn = value
	return &opts
}

func (o *options) NewPersistManagerFn() storage.NewPersistManagerFn {
	return o.newPersistManagerFn
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
