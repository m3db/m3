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

package cache

import (
	"time"

	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

// Configuration is config used to create a matcher.Cache.
type Configuration struct {
	Capacity          int           `yaml:"capacity"`
	FreshDuration     time.Duration `yaml:"freshDuration"`
	StutterDuration   time.Duration `yaml:"stutterDuration"`
	EvictionBatchSize int           `yaml:"evictionBatchSize"`
	DeletionBatchSize int           `yaml:"deletionBatchSize"`
}

// NewCache creates a matcher.Cache.
func (cfg *Configuration) NewCache(
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) matcher.Cache {
	opts := NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts)
	if cfg.Capacity != 0 {
		opts = opts.SetCapacity(cfg.Capacity)
	}
	if cfg.FreshDuration != 0 {
		opts = opts.SetFreshDuration(cfg.FreshDuration)
	}
	if cfg.StutterDuration != 0 {
		opts = opts.SetStutterDuration(cfg.StutterDuration)
	}
	if cfg.EvictionBatchSize != 0 {
		opts = opts.SetEvictionBatchSize(cfg.EvictionBatchSize)
	}
	if cfg.DeletionBatchSize != 0 {
		opts = opts.SetDeletionBatchSize(cfg.DeletionBatchSize)
	}

	return NewCache(opts)
}
