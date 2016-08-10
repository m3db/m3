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
	"time"

	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
)

const (
	// defaultThrottlePeriod is how long we wait till the next iteration of bootstrap starts by default.
	defaultThrottlePeriod = 10 * time.Second

	// defaultMaxRetries is the maximum number of bootstrap retries by default.
	defaultMaxRetries = 3
)

type options struct {
	instrumentOpts instrument.Options
	retentionOpts  retention.Options
	blockOpts      block.Options
	throttlePeriod time.Duration
	maxRetries     int
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		instrumentOpts: instrument.NewOptions(),
		retentionOpts:  retention.NewOptions(),
		blockOpts:      block.NewOptions(),
		throttlePeriod: defaultThrottlePeriod,
		maxRetries:     defaultMaxRetries,
	}
}

func (o *options) InstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) GetInstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) RetentionOptions(value retention.Options) Options {
	opts := *o
	opts.retentionOpts = value
	return &opts
}

func (o *options) GetRetentionOptions() retention.Options {
	return o.retentionOpts
}

func (o *options) DatabaseBlockOptions(value block.Options) Options {
	opts := *o
	opts.blockOpts = value
	return &opts
}

func (o *options) GetDatabaseBlockOptions() block.Options {
	return o.blockOpts
}

func (o *options) ThrottlePeriod(value time.Duration) Options {
	opts := *o
	opts.throttlePeriod = value
	return &opts
}

func (o *options) GetThrottlePeriod() time.Duration {
	return o.throttlePeriod
}

func (o *options) MaxRetries(value int) Options {
	opts := *o
	opts.maxRetries = value
	return &opts
}

func (o *options) GetMaxRetries() int {
	return o.maxRetries
}
