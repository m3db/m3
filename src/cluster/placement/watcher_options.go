// Copyright (c) 2021 Uber Technologies, Inc.
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

package placement

import (
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/x/instrument"
)

var _ WatcherOptions = (*watcherOptions)(nil) // enforce interface compliance

const (
	defaultInitWatchTimeout = 10 * time.Second
)

type watcherOptions struct {
	instrumentOpts       instrument.Options
	stagedPlacementKey   string
	stagedPlacementStore kv.Store
	initWatchTimeout     time.Duration
	onPlacementChangedFn OnPlacementChangedFn
}

// NewWatcherOptions create a new set of options.
func NewWatcherOptions() WatcherOptions {
	return &watcherOptions{
		instrumentOpts:   instrument.NewOptions(),
		initWatchTimeout: defaultInitWatchTimeout,
	}
}

func (o *watcherOptions) SetInstrumentOptions(value instrument.Options) WatcherOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *watcherOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *watcherOptions) SetStagedPlacementKey(value string) WatcherOptions {
	opts := *o
	opts.stagedPlacementKey = value
	return &opts
}

func (o *watcherOptions) StagedPlacementKey() string {
	return o.stagedPlacementKey
}

func (o *watcherOptions) SetStagedPlacementStore(value kv.Store) WatcherOptions {
	opts := *o
	opts.stagedPlacementStore = value
	return &opts
}

func (o *watcherOptions) StagedPlacementStore() kv.Store {
	return o.stagedPlacementStore
}

func (o *watcherOptions) SetInitWatchTimeout(value time.Duration) WatcherOptions {
	opts := *o
	opts.initWatchTimeout = value
	return &opts
}

func (o *watcherOptions) InitWatchTimeout() time.Duration {
	return o.initWatchTimeout
}

func (o *watcherOptions) SetOnPlacementChangedFn(value OnPlacementChangedFn) WatcherOptions {
	opts := *o
	opts.onPlacementChangedFn = value
	return &opts
}

func (o *watcherOptions) OnPlacementChangedFn() OnPlacementChangedFn {
	return o.onPlacementChangedFn
}
