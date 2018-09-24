// Copyright (c) 2017 Uber Technologies, Inc.
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

import "github.com/m3db/m3/src/dbnode/topology"

var (
	// defaultPersistConfig declares the intent to by default to perform
	// a bootstrap without persistence enabled.
	defaultPersistConfig = PersistConfig{
		Enabled: false,
	}
)

type runOptions struct {
	persistConfig        PersistConfig
	cacheSeriesMetadata  bool
	initialTopologyState *topology.StateSnapshot
}

// NewRunOptions creates new bootstrap run options
func NewRunOptions() RunOptions {
	return &runOptions{
		persistConfig:        defaultPersistConfig,
		cacheSeriesMetadata:  defaultCacheSeriesMetadata,
		initialTopologyState: nil,
	}
}

func (o *runOptions) SetPersistConfig(value PersistConfig) RunOptions {
	opts := *o
	opts.persistConfig = value
	return &opts
}

func (o *runOptions) PersistConfig() PersistConfig {
	return o.persistConfig
}

func (o *runOptions) SetCacheSeriesMetadata(value bool) RunOptions {
	opts := *o
	opts.cacheSeriesMetadata = value
	return &opts
}

func (o *runOptions) CacheSeriesMetadata() bool {
	return o.cacheSeriesMetadata
}

func (o *runOptions) SetInitialTopologyState(value *topology.StateSnapshot) RunOptions {
	opts := *o
	opts.initialTopologyState = value
	return &opts
}

func (o *runOptions) InitialTopologyState() *topology.StateSnapshot {
	return o.initialTopologyState
}
