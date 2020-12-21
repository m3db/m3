// Copyright (c) 2020 Uber Technologies, Inc.
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

package etcd

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	opts := NewOptions()
	assert.NoError(t, opts.Validate())
	assert.Equal(t, defaultRequestTimeout, opts.RequestTimeout())
	assert.Equal(t, defaultWatchChanCheckInterval, opts.WatchChanCheckInterval())
	assert.Equal(t, defaultWatchChanResetInterval, opts.WatchChanCheckInterval())
	assert.Equal(t, defaultWatchChanInitTimeout, opts.WatchChanInitTimeout())
	assert.False(t, opts.EnableFastGets())
	ropts := opts.RetryOptions()
	assert.Equal(t, true, ropts.Jitter())
	assert.Equal(t, time.Second, ropts.InitialBackoff())
	assert.EqualValues(t, 2, ropts.BackoffFactor())
	assert.EqualValues(t, 5, ropts.MaxRetries())
	assert.Equal(t, time.Duration(math.MaxInt64), ropts.MaxBackoff())
}
