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
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestRuntimeOptionsManagerUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	limit := int64(100)
	opts := NewOptions().SetWriteValuesPerMetricLimitPerSecond(limit)
	mgr := NewOptionsManager(opts)
	assert.Equal(t, limit, mgr.RuntimeOptions().WriteValuesPerMetricLimitPerSecond())

	var (
		res     Options
		resLock sync.Mutex
	)
	w := NewMockOptionsWatcher(ctrl)
	w.EXPECT().
		SetRuntimeOptions(gomock.Any()).
		Do(func(value Options) {
			resLock.Lock()
			res = value
			resLock.Unlock()
		}).
		AnyTimes()

	resLock.Lock()
	assert.Nil(t, res)
	resLock.Unlock()

	// Ensure immediately sets the value.
	mgr.RegisterWatcher(w)
	resLock.Lock()
	assert.Equal(t, opts, res)
	resLock.Unlock()

	// Update and verify.
	newLimit := int64(200)
	mgr.SetRuntimeOptions(opts.SetWriteValuesPerMetricLimitPerSecond(newLimit))

	// Verify watcher receives update.
	for {
		resLock.Lock()
		rl := res.WriteValuesPerMetricLimitPerSecond()
		resLock.Unlock()
		if rl == newLimit {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}
