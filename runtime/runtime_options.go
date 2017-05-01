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

package runtime

import (
	"time"

	"github.com/m3db/m3db/ratelimit"
)

const (
	// defaultWriteNewSeriesAsync is false by default to give consistent
	// writes by default
	defaultWriteNewSeriesAsync = false

	// defaultMaxWiredBlocks is 2MM by default which if using 2hr block sizes
	// and writing every 10s at 1.4 point/byte (m3tsz) should use roughly 4gb:
	// 1.4 * 6 * 120 * (2^21) = ~2gb
	defaultMaxWiredBlocks = 2097152

	// defaultWiredBlockExpiryAfterNotAccessedPeriod is disabled by default
	defaultWiredBlockExpiryAfterNotAccessedPeriod = 0
)

type options struct {
	persistRateLimitOpts                   ratelimit.Options
	writeNewSeriesAsync                    bool
	maxWiredBlocks                         int
	wiredBlockExpiryAfterNotAccessedPeriod time.Duration
}

// NewOptions creates a new set of runtime options with defaults
func NewOptions() Options {
	return &options{
		persistRateLimitOpts:                   ratelimit.NewOptions(),
		writeNewSeriesAsync:                    defaultWriteNewSeriesAsync,
		maxWiredBlocks:                         defaultMaxWiredBlocks,
		wiredBlockExpiryAfterNotAccessedPeriod: defaultWiredBlockExpiryAfterNotAccessedPeriod,
	}
}

func (o *options) SetPersistRateLimitOptions(value ratelimit.Options) Options {
	opts := *o
	opts.persistRateLimitOpts = value
	return &opts
}

func (o *options) PersistRateLimitOptions() ratelimit.Options {
	return o.persistRateLimitOpts
}

func (o *options) SetWriteNewSeriesAsync(value bool) Options {
	opts := *o
	opts.writeNewSeriesAsync = value
	return &opts
}

func (o *options) WriteNewSeriesAsync() bool {
	return o.writeNewSeriesAsync
}

func (o *options) SetMaxWiredBlocks(value int) Options {
	opts := *o
	opts.maxWiredBlocks = value
	return &opts
}

func (o *options) MaxWiredBlocks() int {
	return o.maxWiredBlocks
}

func (o *options) SetWiredBlockExpiryAfterNotAccessedPeriod(value time.Duration) Options {
	opts := *o
	opts.wiredBlockExpiryAfterNotAccessedPeriod = value
	return &opts
}

func (o *options) WiredBlockExpiryAfterNotAccessedPeriod() time.Duration {
	return o.wiredBlockExpiryAfterNotAccessedPeriod
}
