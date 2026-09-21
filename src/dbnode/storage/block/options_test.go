// Copyright (c) 2024 Uber Technologies, Inc.
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

package block

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/pool"
	xsync "github.com/m3db/m3/src/x/sync"
)

func TestOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("NewOptions", func(t *testing.T) {
		o := NewOptions()
		require.NotNil(t, o)

		// Verify default values
		require.Equal(t, defaultDatabaseBlockAllocSize, o.DatabaseBlockAllocSize())
		require.NotNil(t, o.ClockOptions())
		require.NotNil(t, o.CloseContextWorkers())
		require.NotNil(t, o.DatabaseBlockPool())
		require.NotNil(t, o.ContextPool())
		require.NotNil(t, o.EncoderPool())
		require.NotNil(t, o.ReaderIteratorPool())
		require.NotNil(t, o.MultiReaderIteratorPool())
		require.NotNil(t, o.SegmentReaderPool())
		require.NotNil(t, o.BytesPool())
		require.Nil(t, o.WiredList())
	})

	t.Run("ClockOptions", func(t *testing.T) {
		o := NewOptions()
		clockOpts := clock.NewOptions().SetNowFn(time.Now)
		o = o.SetClockOptions(clockOpts)
		require.Equal(t, clockOpts, o.ClockOptions())
	})

	t.Run("DatabaseBlockAllocSize", func(t *testing.T) {
		o := NewOptions()
		size := 2048
		o = o.SetDatabaseBlockAllocSize(size)
		require.Equal(t, size, o.DatabaseBlockAllocSize())
	})

	t.Run("CloseContextWorkers", func(t *testing.T) {
		o := NewOptions()
		workers := xsync.NewWorkerPool(8)
		workers.Init()
		o = o.SetCloseContextWorkers(workers)
		require.Equal(t, workers, o.CloseContextWorkers())
	})

	t.Run("DatabaseBlockPool", func(t *testing.T) {
		o := NewOptions()
		pool := NewMockDatabaseBlockPool(ctrl)
		o = o.SetDatabaseBlockPool(pool)
		require.Equal(t, pool, o.DatabaseBlockPool())
	})

	t.Run("ContextPool", func(t *testing.T) {
		o := NewOptions()
		pool := context.NewPool(context.NewOptions().SetContextPoolOptions(pool.NewObjectPoolOptions()))
		o = o.SetContextPool(pool)
		require.Equal(t, pool, o.ContextPool())
	})

	t.Run("EncoderPool", func(t *testing.T) {
		o := NewOptions()
		pool := encoding.NewMockEncoderPool(ctrl)
		o = o.SetEncoderPool(pool)
		require.Equal(t, pool, o.EncoderPool())
	})

	t.Run("ReaderIteratorPool", func(t *testing.T) {
		o := NewOptions()
		pool := encoding.NewMockReaderIteratorPool(ctrl)
		o = o.SetReaderIteratorPool(pool)
		require.Equal(t, pool, o.ReaderIteratorPool())
	})

	t.Run("MultiReaderIteratorPool", func(t *testing.T) {
		o := NewOptions()
		pool := encoding.NewMockMultiReaderIteratorPool(ctrl)
		o = o.SetMultiReaderIteratorPool(pool)
		require.Equal(t, pool, o.MultiReaderIteratorPool())
	})

	t.Run("SegmentReaderPool", func(t *testing.T) {
		o := NewOptions()
		pool := xio.NewMockSegmentReaderPool(ctrl)
		o = o.SetSegmentReaderPool(pool)
		require.Equal(t, pool, o.SegmentReaderPool())
	})

	t.Run("BytesPool", func(t *testing.T) {
		o := NewOptions()
		pool := pool.NewMockCheckedBytesPool(ctrl)
		o = o.SetBytesPool(pool)
		require.Equal(t, pool, o.BytesPool())
	})

	t.Run("WiredList", func(t *testing.T) {
		o := NewOptions()
		wiredList := &WiredList{}
		o = o.SetWiredList(wiredList)
		require.Equal(t, wiredList, o.WiredList())
	})

	t.Run("Immutability", func(t *testing.T) {
		o := NewOptions()
		require.NotEqual(t, o, o.SetClockOptions(clock.NewOptions()))
		require.NotEqual(t, o, o.SetDatabaseBlockAllocSize(2048))
		require.NotEqual(t, o, o.SetCloseContextWorkers(xsync.NewWorkerPool(8)))
		require.NotEqual(t, o, o.SetDatabaseBlockPool(NewMockDatabaseBlockPool(ctrl)))
		require.NotEqual(t, o, o.SetContextPool(context.NewPool(context.NewOptions())))
		require.NotEqual(t, o, o.SetEncoderPool(encoding.NewMockEncoderPool(ctrl)))
		require.NotEqual(t, o, o.SetReaderIteratorPool(encoding.NewMockReaderIteratorPool(ctrl)))
		require.NotEqual(t, o, o.SetMultiReaderIteratorPool(encoding.NewMockMultiReaderIteratorPool(ctrl)))
		require.NotEqual(t, o, o.SetSegmentReaderPool(xio.NewMockSegmentReaderPool(ctrl)))
		require.NotEqual(t, o, o.SetBytesPool(pool.NewMockCheckedBytesPool(ctrl)))
		require.NotEqual(t, o, o.SetWiredList(&WiredList{}))
	})
}
