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

package placement

import (
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/instrument"
)

// WatcherConfiguration contains placement watcher configuration.
type WatcherConfiguration struct {
	// Placement key.
	Key string `yaml:"key" validate:"nonzero"`

	// Initial watch timeout.
	InitWatchTimeout time.Duration `yaml:"initWatchTimeout"`
}

// NewOptions creates a placement watcher option.
func (c *WatcherConfiguration) NewOptions(
	store kv.Store,
	instrumentOpts instrument.Options,
) services.StagedPlacementWatcherOptions {
	opts := NewStagedPlacementWatcherOptions().
		SetInstrumentOptions(instrumentOpts).
		SetStagedPlacementKey(c.Key).
		SetStagedPlacementStore(store)
	if c.InitWatchTimeout != 0 {
		opts = opts.SetInitWatchTimeout(c.InitWatchTimeout)
	}
	return opts
}
