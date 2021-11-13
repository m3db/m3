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

package writer

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestMessageWriterRandomIndex(t *testing.T) {
	indexes := make([]int, 10)
	reset := func() {
		for i := range indexes {
			indexes[i] = i
		}
	}

	reset()
	firstIdx := randIndex(indexes, len(indexes)-1)
	for {
		reset()
		newIdx := randIndex(indexes, len(indexes)-1)
		// Make sure the first index is random.
		if firstIdx != newIdx {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	reset()
	idx1 := randIndex(indexes, len(indexes)-1)
	idx2 := randIndex(indexes, len(indexes)-2)
	for {
		reset()
		newIdx1 := randIndex(indexes, len(indexes)-1)
		newIdx2 := randIndex(indexes, len(indexes)-2)
		// Make sure the order is random.
		if idx2-idx1 != newIdx2-newIdx1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestMessageWriterRandomFullIteration(t *testing.T) {
	indexes := make([]int, 100)
	var indexMap map[int]struct{}
	reset := func() {
		for i := range indexes {
			indexes[i] = i
		}
		indexMap = make(map[int]struct{}, 100)
	}

	for n := 0; n < 1000; n++ {
		reset()
		for i := len(indexes) - 1; i >= 0; i-- {
			idx := randIndex(indexes, i)
			indexMap[idx] = struct{}{}
		}
		require.Equal(t, 100, len(indexMap))
	}
}

func TestMessageWriterWithPooling(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	addr := lis.Addr().String()
	opts := testOptions()

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics()).(*messageWriterImpl)
	require.Equal(t, 200, int(w.ReplicatedShardID()))
	w.Init()

	a := newAckRouter(1)
	a.Register(200, w)

	cw := newConsumerWriter(addr, a, opts, testConsumerWriterMetrics())
	cw.Init()
	defer cw.Close()

	w.AddConsumerWriter(cw)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Bytes().Return([]byte("foo")).Times(1)
	mm1.EXPECT().Size().Return(3).Times(1)
	mm1.EXPECT().Finalize(producer.Consumed)

	w.Write(producer.NewRefCountedMessage(mm1, nil))

	for {
		w.RLock()
		l := w.queue.Len()
		w.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, 0, w.queue.Len())
	w.RemoveConsumerWriter(addr)

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Bytes().Return([]byte("bar")).Times(1)
	mm2.EXPECT().Size().Return(3).Times(1)

	w.Write(producer.NewRefCountedMessage(mm2, nil))
	for {
		if !isEmptyWithLock(w.acks) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, 1, w.queue.Len())

	mm2.EXPECT().Finalize(producer.Consumed)
	w.Ack(metadata{metadataKey: metadataKey{shard: 200, id: 2}})
	require.True(t, isEmptyWithLock(w.acks))
	for {
		w.RLock()
		l := w.queue.Len()
		w.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	w.Close()
	w.Close()
}

func TestMessageWriterWithoutPooling(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	addr := lis.Addr().String()
	opts := testOptions()

	var wg sync.WaitGroup
	defer wg.Wait()

	wg.Add(1)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncoderOptions(), opts.DecoderOptions())
		wg.Done()
	}()

	w := newMessageWriter(200, nil, opts, testMessageWriterMetrics()).(*messageWriterImpl)
	require.Equal(t, 200, int(w.ReplicatedShardID()))
	w.Init()

	a := newAckRouter(1)
	a.Register(200, w)

	cw := newConsumerWriter(addr, a, opts, testConsumerWriterMetrics())
	cw.Init()
	defer cw.Close()

	w.AddConsumerWriter(cw)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Bytes().Return([]byte("foo")).Times(1)
	mm1.EXPECT().Size().Return(3).Times(1)
	mm1.EXPECT().Finalize(producer.Consumed)

	w.Write(producer.NewRefCountedMessage(mm1, nil))

	for {
		w.RLock()
		l := w.queue.Len()
		w.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, 0, w.queue.Len())
	w.RemoveConsumerWriter(addr)

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Bytes().Return([]byte("bar")).Times(1)
	mm2.EXPECT().Size().Return(3).Times(1)

	w.Write(producer.NewRefCountedMessage(mm2, nil))
	for {
		if !isEmptyWithLock(w.acks) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, 1, w.queue.Len())

	mm2.EXPECT().Finalize(producer.Consumed)
	w.Ack(metadata{metadataKey: metadataKey{shard: 200, id: 2}})
	require.True(t, isEmptyWithLock(w.acks))
	for {
		w.RLock()
		l := w.queue.Len()
		w.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	w.Close()
	w.Close()
}

func TestMessageWriterRetryWithoutPooling(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	addr := lis.Addr().String()
	opts := testOptions()
	w := newMessageWriter(200, nil, opts, testMessageWriterMetrics()).(*messageWriterImpl)
	w.Init()
	defer w.Close()

	a := newAckRouter(1)
	a.Register(200, w)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Bytes().Return([]byte("foo")).AnyTimes()
	mm.EXPECT().Size().Return(3).Times(1)
	mm.EXPECT().Finalize(producer.Consumed)

	rm := producer.NewRefCountedMessage(mm, nil)
	w.Write(rm)

	w.AddConsumerWriter(newConsumerWriter("bad", a, opts, testConsumerWriterMetrics()))
	require.Equal(t, 1, w.queue.Len())

	for {
		if !isEmptyWithLock(w.acks) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	_, ok := w.acks.ackMap[metadataKey{shard: 200, id: 1}]
	require.True(t, ok)

	cw := newConsumerWriter(addr, a, opts, testConsumerWriterMetrics())
	cw.Init()
	defer cw.Close()

	w.AddConsumerWriter(cw)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncoderOptions(), opts.DecoderOptions())
	}()

	for {
		w.Lock()
		l := w.queue.Len()
		w.Unlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestMessageWriterRetryWithPooling(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	addr := lis.Addr().String()
	opts := testOptions()
	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics()).(*messageWriterImpl)
	w.Init()
	defer w.Close()

	a := newAckRouter(1)
	a.Register(200, w)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Bytes().Return([]byte("foo")).AnyTimes()
	mm.EXPECT().Size().Return(3).Times(1)
	mm.EXPECT().Finalize(producer.Consumed)

	rm := producer.NewRefCountedMessage(mm, nil)
	w.Write(rm)

	w.AddConsumerWriter(newConsumerWriter("bad", a, opts, testConsumerWriterMetrics()))
	require.Equal(t, 1, w.queue.Len())

	for {
		if !isEmptyWithLock(w.acks) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	m1, ok := w.acks.ackMap[metadataKey{shard: 200, id: 1}]
	require.True(t, ok)

	cw := newConsumerWriter(addr, a, opts, testConsumerWriterMetrics())
	cw.Init()
	defer cw.Close()

	w.AddConsumerWriter(cw)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncoderOptions(), opts.DecoderOptions())
	}()

	for {
		w.Lock()
		l := w.queue.Len()
		w.Unlock()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// A get will NOT allocate a new message because the old one has been returned to pool.
	m := w.mPool.Get()
	require.Equal(t, m1, m)
	require.True(t, m.IsDroppedOrConsumed())
}

func TestMessageWriterCleanupDroppedMessage(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics())

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)

	mm.EXPECT().Size().Return(3).Times(1)
	rm := producer.NewRefCountedMessage(mm, nil)
	mm.EXPECT().Finalize(producer.Dropped)
	rm.Drop()
	mm.EXPECT().Bytes().Return([]byte("foo"))
	w.Write(rm)

	// A get will allocate a new message because the old one has not been returned to pool yet.
	m := w.(*messageWriterImpl).mPool.Get()
	require.Nil(t, m.RefCountedMessage)

	require.Equal(t, 1, w.(*messageWriterImpl).queue.Len())
	w.Init()
	defer w.Close()

	for {
		w.(*messageWriterImpl).Lock()
		l := w.(*messageWriterImpl).queue.Len()
		w.(*messageWriterImpl).Unlock()
		if l != 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, isEmptyWithLock(w.(*messageWriterImpl).acks))

	// A get will NOT allocate a new message because the old one has been returned to pool.
	m = w.(*messageWriterImpl).mPool.Get()
	require.True(t, m.IsDroppedOrConsumed())
}

func TestMessageWriterCleanupAckedMessage(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics()).(*messageWriterImpl)
	w.Init()
	defer w.Close()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Bytes().Return([]byte("foo"))
	mm.EXPECT().Size().Return(3).Times(1)

	rm := producer.NewRefCountedMessage(mm, nil)
	// Another message write also holds this message.
	rm.IncRef()

	w.Write(rm)
	for {
		if !isEmptyWithLock(w.acks) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	acks := w.acks
	meta := metadata{
		metadataKey: metadataKey{
			id:    1,
			shard: 200,
		},
	}
	// The message will not be finalized because it's still being hold by another message writer.
	acks.ack(meta)
	require.True(t, isEmptyWithLock(w.acks))

	// A get will allocate a new message because the old one has not been returned to pool yet.
	m := w.mPool.Get()
	require.Nil(t, m.RefCountedMessage)
	require.Equal(t, 1, w.queue.Len())

	for {
		w.Lock()
		l := w.queue.Len()
		w.Unlock()
		if l != 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// A get will NOT allocate a new message because the old one has been returned to pool.
	m = w.mPool.Get()
	require.Equal(t, meta, m.Metadata())
}

func TestMessageWriterCutoverCutoff(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	w := newMessageWriter(200, testMessagePool(testOptions()), nil, testMessageWriterMetrics()).(*messageWriterImpl)
	now := time.Now()
	w.nowFn = func() time.Time { return now }
	require.True(t, w.isValidWriteWithLock(now.UnixNano()))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+150))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+250))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+50))

	w.SetCutoffNanos(now.UnixNano() + 200)
	w.SetCutoverNanos(now.UnixNano() + 100)
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+150))
	require.False(t, w.isValidWriteWithLock(now.UnixNano()+250))
	require.False(t, w.isValidWriteWithLock(now.UnixNano()+50))
	require.Equal(t, 0, w.queue.Len())

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(3)
	w.Write(producer.NewRefCountedMessage(mm, nil))
	require.Equal(t, 0, w.queue.Len())
}

func TestMessageWriterIgnoreCutoverCutoff(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions().SetIgnoreCutoffCutover(true)

	w := newMessageWriter(200, testMessagePool(testOptions()), opts, testMessageWriterMetrics()).(*messageWriterImpl)
	now := time.Now()
	w.nowFn = func() time.Time { return now }

	w.SetCutoffNanos(now.UnixNano() + 200)
	w.SetCutoverNanos(now.UnixNano() + 100)
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+150))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+250))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+50))
	require.Equal(t, 0, w.queue.Len())

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Bytes().Return([]byte("foo"))
	mm.EXPECT().Size().Return(3)
	w.Write(producer.NewRefCountedMessage(mm, nil))
	require.Equal(t, 1, w.queue.Len())
}

func TestMessageWriterKeepNewWritesInOrderInFrontOfTheQueue(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := testOptions().SetMessageRetryOptions(
		retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond),
	)
	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics()).(*messageWriterImpl)

	now := time.Now()
	w.nowFn = func() time.Time { return now }

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(3)
	rm1 := producer.NewRefCountedMessage(mm1, nil)
	mm1.EXPECT().Bytes().Return([]byte("1")).AnyTimes()
	w.Write(rm1)
	validateMessages(t, []*producer.RefCountedMessage{rm1}, w)
	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(3)
	rm2 := producer.NewRefCountedMessage(mm2, nil)
	mm2.EXPECT().Bytes().Return([]byte("2")).AnyTimes()
	w.Write(rm2)
	validateMessages(t, []*producer.RefCountedMessage{rm1, rm2}, w)
	w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), 2, true, &scanBatchMetrics{})

	w.lastNewWrite = nil
	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(3)
	rm3 := producer.NewRefCountedMessage(mm3, nil)
	mm3.EXPECT().Bytes().Return([]byte("3")).AnyTimes()
	w.Write(rm3)
	validateMessages(t, []*producer.RefCountedMessage{rm3, rm1, rm2}, w)

	mm4 := producer.NewMockMessage(ctrl)
	mm4.EXPECT().Size().Return(3)
	rm4 := producer.NewRefCountedMessage(mm4, nil)
	mm4.EXPECT().Bytes().Return([]byte("4")).AnyTimes()
	w.Write(rm4)

	validateMessages(t, []*producer.RefCountedMessage{rm3, rm4, rm1, rm2}, w)
}

func TestMessageWriterRetryIterateBatchFullScan(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retryBatchSize := 2
	opts := testOptions().SetMessageQueueScanBatchSize(retryBatchSize).SetMessageRetryOptions(
		retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond),
	)
	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics()).(*messageWriterImpl)

	now := time.Now()
	w.nowFn = func() time.Time { return now }

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(3)
	rm1 := producer.NewRefCountedMessage(mm1, nil)
	mm1.EXPECT().Bytes().Return([]byte("1")).AnyTimes()
	w.Write(rm1)

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(3)
	rm2 := producer.NewRefCountedMessage(mm2, nil)
	mm2.EXPECT().Bytes().Return([]byte("2")).AnyTimes()
	w.Write(rm2)

	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(3)
	rm3 := producer.NewRefCountedMessage(mm3, nil)
	mm3.EXPECT().Bytes().Return([]byte("3")).AnyTimes()
	w.Write(rm3)

	mm4 := producer.NewMockMessage(ctrl)
	mm4.EXPECT().Size().Return(3)
	rm4 := producer.NewRefCountedMessage(mm4, nil)
	mm4.EXPECT().Bytes().Return([]byte("4")).AnyTimes()
	w.Write(rm4)

	validateMessages(t, []*producer.RefCountedMessage{rm1, rm2, rm3, rm4}, w)
	mm1.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	rm1.Drop()
	require.Equal(t, 4, w.queue.Len())
	e, toBeRetried := w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), retryBatchSize, true, &scanBatchMetrics{})
	require.Equal(t, 1, len(toBeRetried))
	require.Equal(t, 3, w.queue.Len())

	// Make sure it stopped at rm3.
	require.Equal(t, rm3, e.Value.(*message).RefCountedMessage)

	require.Equal(t, 3, w.queue.Len())
	e, toBeRetried = w.scanBatchWithLock(e, w.nowFn().UnixNano(), retryBatchSize, true, &scanBatchMetrics{})
	require.Nil(t, e)
	require.Equal(t, 2, len(toBeRetried))
	require.Equal(t, 3, w.queue.Len())

	e, toBeRetried = w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), retryBatchSize, true, &scanBatchMetrics{})
	// Make sure it stopped at rm4.
	require.Equal(t, rm4, e.Value.(*message).RefCountedMessage)
	require.Equal(t, 0, len(toBeRetried))

	e, toBeRetried = w.scanBatchWithLock(e, w.nowFn().UnixNano(), retryBatchSize, true, &scanBatchMetrics{})
	require.Nil(t, e)
	require.Equal(t, 0, len(toBeRetried))
}

//nolint:lll
func TestMessageWriterRetryIterateBatchFullScanWithMessageTTL(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retryBatchSize := 2
	opts := testOptions().SetMessageQueueScanBatchSize(retryBatchSize).SetMessageRetryOptions(
		retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond),
	)
	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics()).(*messageWriterImpl)

	now := time.Now()
	w.nowFn = func() time.Time { return now }

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(3)
	rm1 := producer.NewRefCountedMessage(mm1, nil)
	mm1.EXPECT().Bytes().Return([]byte("1")).AnyTimes()
	w.Write(rm1)

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(3)
	rm2 := producer.NewRefCountedMessage(mm2, nil)
	mm2.EXPECT().Bytes().Return([]byte("2")).AnyTimes()
	w.Write(rm2)

	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(3)
	rm3 := producer.NewRefCountedMessage(mm3, nil)
	mm3.EXPECT().Bytes().Return([]byte("3")).AnyTimes()
	w.Write(rm3)

	mm4 := producer.NewMockMessage(ctrl)
	mm4.EXPECT().Size().Return(3)
	rm4 := producer.NewRefCountedMessage(mm4, nil)
	mm4.EXPECT().Bytes().Return([]byte("4")).AnyTimes()
	w.Write(rm4)

	mm1.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	rm1.Drop()
	require.Equal(t, 4, w.queue.Len())
	e, toBeRetried := w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), retryBatchSize, true, &scanBatchMetrics{})
	require.Equal(t, 1, len(toBeRetried))
	require.Equal(t, 3, w.queue.Len())

	require.Equal(t, rm2, e.Value.(*message).RefCountedMessage)

	w.SetMessageTTLNanos(int64(time.Minute))
	mm4.EXPECT().Finalize(gomock.Eq(producer.Consumed))
	mm3.EXPECT().Finalize(gomock.Eq(producer.Consumed))
	e, toBeRetried = w.scanBatchWithLock(e, w.nowFn().UnixNano()+int64(time.Hour), retryBatchSize, true, &scanBatchMetrics{})
	require.Equal(t, 0, len(toBeRetried))
	require.Equal(t, 1, w.queue.Len())
	require.Nil(t, e)

	mm2.EXPECT().Finalize(gomock.Eq(producer.Consumed))
	e, toBeRetried = w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano()+int64(time.Hour), retryBatchSize, true, &scanBatchMetrics{})
	require.Equal(t, 0, len(toBeRetried))
	require.Equal(t, 0, w.queue.Len())
	require.Nil(t, e)
}

//nolint:lll
func TestMessageWriterRetryIterateBatchNotFullScan(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retryBatchSize := 100
	opts := testOptions().SetMessageQueueScanBatchSize(retryBatchSize).SetMessageRetryOptions(
		retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond),
	)
	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics()).(*messageWriterImpl)

	now := time.Now()
	w.nowFn = func() time.Time { return now }

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(1)
	rm1 := producer.NewRefCountedMessage(mm1, nil)
	mm1.EXPECT().Bytes().Return([]byte("1")).AnyTimes()
	w.Write(rm1)

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(1)
	rm2 := producer.NewRefCountedMessage(mm2, nil)
	mm2.EXPECT().Bytes().Return([]byte("2")).AnyTimes()
	w.Write(rm2)

	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(1)
	rm3 := producer.NewRefCountedMessage(mm3, nil)
	mm3.EXPECT().Bytes().Return([]byte("3")).AnyTimes()
	w.Write(rm3)

	mm4 := producer.NewMockMessage(ctrl)
	mm4.EXPECT().Size().Return(1)
	rm4 := producer.NewRefCountedMessage(mm4, nil)
	mm4.EXPECT().Bytes().Return([]byte("4")).AnyTimes()
	w.Write(rm4)

	mm1.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	rm1.Drop()
	require.Equal(t, 4, w.queue.Len())
	e, toBeRetried := w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), retryBatchSize, false, &scanBatchMetrics{})
	require.Equal(t, 3, len(toBeRetried))
	require.Equal(t, 3, w.queue.Len())
	require.Nil(t, e)
	validateMessages(t, []*producer.RefCountedMessage{rm2, rm3, rm4}, w)
	// Although mm4 is dropped, it will not be removed from the queue because
	// it was not checked.
	mm4.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	rm4.Drop()
	require.Equal(t, 3, w.queue.Len())
	e, toBeRetried = w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), retryBatchSize, false, &scanBatchMetrics{})
	require.Equal(t, rm2, e.Value.(*message).RefCountedMessage)
	require.Equal(t, 0, len(toBeRetried))
	require.Equal(t, 3, w.queue.Len())
	require.Equal(t, rm2, e.Value.(*message).RefCountedMessage)
	validateMessages(t, []*producer.RefCountedMessage{rm2, rm3, rm4}, w)
	w.lastNewWrite = nil

	mm5 := producer.NewMockMessage(ctrl)
	mm5.EXPECT().Size().Return(1)
	rm5 := producer.NewRefCountedMessage(mm5, nil)
	mm5.EXPECT().Bytes().Return([]byte("5")).AnyTimes()
	w.Write(rm5)
	validateMessages(t, []*producer.RefCountedMessage{rm5, rm2, rm3, rm4}, w)

	require.Equal(t, 4, w.queue.Len())
	e, toBeRetried = w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), retryBatchSize, false, &scanBatchMetrics{})
	require.Equal(t, rm2, e.Value.(*message).RefCountedMessage)
	require.Equal(t, 1, len(toBeRetried))
	require.Equal(t, rm5, toBeRetried[0].RefCountedMessage)
	require.Equal(t, 4, w.queue.Len())
}

func TestNextRetryAfterNanos(t *testing.T) {
	backoffDuration := time.Minute
	opts := testOptions().SetMessageRetryOptions(
		retry.NewOptions().SetInitialBackoff(backoffDuration).SetMaxBackoff(2 * backoffDuration).SetJitter(true),
	)
	w := newMessageWriter(200, nil, opts, testMessageWriterMetrics()).(*messageWriterImpl)

	nowNanos := time.Now().UnixNano()
	m := newMessage()
	m.IncWriteTimes()
	retryAtNanos := w.nextRetryAfterNanos(m.WriteTimes()) + nowNanos
	require.True(t, retryAtNanos > nowNanos)
	require.True(t, retryAtNanos < nowNanos+int64(backoffDuration))

	m.IncWriteTimes()
	retryAtNanos = w.nextRetryAfterNanos(m.WriteTimes()) + nowNanos
	require.True(t, retryAtNanos >= nowNanos+int64(backoffDuration))
	require.True(t, retryAtNanos < nowNanos+2*int64(backoffDuration))

	m.IncWriteTimes()
	retryAtNanos = w.nextRetryAfterNanos(m.WriteTimes()) + nowNanos
	require.True(t, retryAtNanos == nowNanos+2*int64(backoffDuration))
}

func TestExpectedProcessedAt(t *testing.T) {
	m := newMessage()
	m.initNanos = 100
	m.SetRetryAtNanos(200)
	require.Equal(t, int64(100), m.ExpectedProcessAtNanos())
	m.SetRetryAtNanos(300)
	require.Equal(t, int64(200), m.ExpectedProcessAtNanos())
}

func TestMessageWriterCloseCleanupAllMessages(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newMessageWriter(200, nil, opts, testMessageWriterMetrics()).(*messageWriterImpl)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(3)

	rm := producer.NewRefCountedMessage(mm, nil)
	mm.EXPECT().Finalize(producer.Consumed)
	mm.EXPECT().Bytes().Return([]byte("foo"))
	w.Write(rm)
	require.False(t, isEmptyWithLock(w.acks))
	require.Equal(t, 1, w.queue.Len())
	w.Init()
	w.Close()
	require.Equal(t, 0, w.queue.Len())
	require.True(t, isEmptyWithLock(w.acks))
}

func TestMessageWriterQueueFullScanOnWriteErrors(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := testOptions().SetMessageQueueScanBatchSize(1)
	scope := tally.NewTestScope("", nil)
	metrics := testMessageWriterMetricsWithScope(scope).withConsumer("c1")
	w := newMessageWriter(200, nil, opts, metrics).(*messageWriterImpl)
	w.AddConsumerWriter(newConsumerWriter("bad", nil, opts, testConsumerWriterMetrics()))

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(3)
	mm1.EXPECT().Bytes().Return([]byte("foo"))
	rm1 := producer.NewRefCountedMessage(mm1, nil)
	w.Write(rm1)
	require.Equal(t, 1, w.queue.Len())

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(3)
	mm2.EXPECT().Bytes().Return([]byte("foo"))
	rm2 := producer.NewRefCountedMessage(mm2, nil)
	w.Write(rm2)
	require.Equal(t, 2, w.queue.Len())

	mm1.EXPECT().Finalize(producer.Dropped)
	rm1.Drop()
	w.scanMessageQueue()
	require.Equal(t, 1, w.queue.Len())

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()
	require.Equal(t, int64(1), counters["message-processed+consumer=c1,result=write"].Value())
	require.Equal(t, int64(1), counters["message-processed+consumer=c1,result=drop"].Value())
}

func TestMessageWriter_WithoutConsumerScope(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := testOptions().SetMessageQueueScanBatchSize(1)
	scope := tally.NewTestScope("", nil)
	metrics := newMessageWriterMetrics(scope, instrument.TimerOptions{}, true)
	w := newMessageWriter(200, nil, opts, metrics).(*messageWriterImpl)
	w.AddConsumerWriter(newConsumerWriter("bad", nil, opts, testConsumerWriterMetrics()))

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()
	require.Nil(t, counters["message-processed+consumer=c1,result=write"])
	require.NotNil(t, counters["message-processed+result=write"])
}

func isEmptyWithLock(h *acks) bool {
	h.Lock()
	defer h.Unlock()
	return len(h.ackMap) == 0
}

func testMessagePool(opts Options) messagePool {
	p := newMessagePool(opts.MessagePoolOptions())
	p.Init()
	return p
}

func testMessageWriterMetrics() messageWriterMetrics {
	return newMessageWriterMetrics(tally.NoopScope, instrument.TimerOptions{}, false)
}

func testMessageWriterMetricsWithScope(scope tally.TestScope) messageWriterMetrics {
	return newMessageWriterMetrics(scope, instrument.TimerOptions{}, false)
}

func validateMessages(t *testing.T, msgs []*producer.RefCountedMessage, w *messageWriterImpl) {
	w.RLock()
	idx := 0
	for e := w.queue.Front(); e != nil; e = e.Next() {
		require.Equal(t, msgs[idx].Bytes(), e.Value.(*message).RefCountedMessage.Bytes())
		idx++
	}
	w.RUnlock()
	require.Equal(t, idx, len(msgs))
}
