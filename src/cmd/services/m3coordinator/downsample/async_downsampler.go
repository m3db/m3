// Copyright (c) 2018 Uber Technologies, Inc.
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

package downsample

import (
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/metrics/rules/view"
)

const (
	errNewDownsamplerFailFmt = "downsampler failed to initialize: %v"
)

var (
	errDownsamplerUninitialized = errors.New("downsampler is not yet initialized")
)

// asyncDownsampler is an asynchronous downsampler that can be lazily
// initialized, it will return errors on calls to its methods until it is
// initialized.
// This is useful when a component needs to be built with a downsampler that
// might not be able to wait for to it be constructed (i.e. HTTP handlers
// need to be registered immediately but can respond with errors until
// the downsampler downstream services have been discovered/connected to).
type asyncDownsampler struct {
	sync.RWMutex
	downsampler Downsampler
	done        chan<- struct{}
	err         error
}

// NewDownsamplerFn creates a downsampler.
type NewDownsamplerFn func() (Downsampler, error)

// NewAsyncDownsampler is a downsampler that is lazily initialized.
func NewAsyncDownsampler(
	fn NewDownsamplerFn,
	done chan<- struct{},
) Downsampler {
	asyncDownsampler := &asyncDownsampler{
		done: done,
		err:  errDownsamplerUninitialized,
	}

	go func() {
		asyncDownsampler.Lock()
		defer asyncDownsampler.Unlock()

		if asyncDownsampler.done != nil {
			defer func() {
				asyncDownsampler.done <- struct{}{}
			}()
		}
		downsampler, err := fn()

		if err != nil {
			asyncDownsampler.err = fmt.Errorf(errNewDownsamplerFailFmt, err)
			return
		}

		asyncDownsampler.downsampler = downsampler
		asyncDownsampler.err = nil
	}()

	return asyncDownsampler
}

func (d *asyncDownsampler) LatestRollupRules(namespace []byte, timeNanos int64) ([]view.RollupRule, error) {
	d.RLock()
	defer d.RUnlock()
	return d.downsampler.LatestRollupRules(namespace, timeNanos)
}

func (d *asyncDownsampler) NewMetricsAppender() (MetricsAppender, error) {
	d.RLock()
	defer d.RUnlock()
	if d.err != nil {
		return nil, d.err
	}
	return d.downsampler.NewMetricsAppender()
}

func (d *asyncDownsampler) Enabled() bool {
	d.RLock()
	defer d.RUnlock()
	if d.err != nil {
		return false
	}
	return d.downsampler.Enabled()
}
