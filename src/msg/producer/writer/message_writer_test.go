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

	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3x/retry"

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
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncodeDecoderOptions())
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

	ctrl := gomock.NewController(t)
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
	w.Ack(metadata{shard: 200, id: 2})
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
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncodeDecoderOptions())
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

	ctrl := gomock.NewController(t)
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
	w.Ack(metadata{shard: 200, id: 2})
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

	ctrl := gomock.NewController(t)
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

	_, ok := w.acks.ackMap[metadata{shard: 200, id: 1}]
	require.True(t, ok)

	cw := newConsumerWriter(addr, a, opts, testConsumerWriterMetrics())
	cw.Init()
	defer cw.Close()

	w.AddConsumerWriter(cw)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncodeDecoderOptions())
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

	ctrl := gomock.NewController(t)
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

	m1, ok := w.acks.ackMap[metadata{shard: 200, id: 1}]
	require.True(t, ok)

	cw := newConsumerWriter(addr, a, opts, testConsumerWriterMetrics())
	cw.Init()
	defer cw.Close()

	w.AddConsumerWriter(cw)
	go func() {
		testConsumeAndAckOnConnectionListener(t, lis, opts.EncodeDecoderOptions())
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

	ctrl := gomock.NewController(t)
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

	ctrl := gomock.NewController(t)
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
		id:    1,
		shard: 200,
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
	ctrl := gomock.NewController(t)
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

func TestMessageWriterRetryIterateBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	retryBatchSize := 2
	opts := testOptions().SetMessageQueueScanBatchSize(retryBatchSize).SetMessageRetryOptions(
		retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond),
	)
	w := newMessageWriter(200, testMessagePool(opts), opts, testMessageWriterMetrics()).(*messageWriterImpl)

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(3)
	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(3)
	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(3)
	mm4 := producer.NewMockMessage(ctrl)
	mm4.EXPECT().Size().Return(3)
	rm1 := producer.NewRefCountedMessage(mm1, nil)
	rm2 := producer.NewRefCountedMessage(mm2, nil)
	rm3 := producer.NewRefCountedMessage(mm3, nil)
	rm4 := producer.NewRefCountedMessage(mm4, nil)
	mm1.EXPECT().Bytes().Return([]byte("1"))
	mm2.EXPECT().Bytes().Return([]byte("2"))
	mm3.EXPECT().Bytes().Return([]byte("3"))
	mm4.EXPECT().Bytes().Return([]byte("4"))
	w.Write(rm1)
	w.Write(rm2)
	w.Write(rm3)
	w.Write(rm4)

	mm4.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	rm4.Drop()
	e, toBeRetried := w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), retryBatchSize)
	require.Equal(t, 2, len(toBeRetried))
	for _, m := range toBeRetried {
		m.SetRetryAtNanos(w.nowFn().Add(time.Hour).UnixNano())
	}
	// Make sure it stopped at rd3.
	mm3.EXPECT().Bytes().Return([]byte("3"))
	require.Equal(t, []byte("3"), e.Value.(*message).RefCountedMessage.Bytes())

	require.Equal(t, 4, w.queue.Len())
	e, toBeRetried = w.scanBatchWithLock(e, w.nowFn().UnixNano(), retryBatchSize)
	require.Nil(t, e)
	require.Equal(t, 1, len(toBeRetried))
	require.Equal(t, 3, w.queue.Len())
	for _, m := range toBeRetried {
		m.SetRetryAtNanos(w.nowFn().Add(time.Hour).UnixNano())
	}
	e, toBeRetried = w.scanBatchWithLock(w.queue.Front(), w.nowFn().UnixNano(), retryBatchSize)
	require.Equal(t, 0, len(toBeRetried))
	// Make sure it stopped at rd3.
	mm3.EXPECT().Bytes().Return([]byte("3"))
	require.Equal(t, []byte("3"), e.Value.(*message).RefCountedMessage.Bytes())
	e, toBeRetried = w.scanBatchWithLock(e, w.nowFn().UnixNano(), retryBatchSize)
	require.Nil(t, e)
	require.Equal(t, 0, len(toBeRetried))
}

func TestNextRetryNanos(t *testing.T) {
	backoffDuration := time.Minute
	opts := testOptions().SetMessageRetryOptions(
		retry.NewOptions().SetInitialBackoff(backoffDuration).SetMaxBackoff(2 * backoffDuration).SetJitter(true),
	)
	w := newMessageWriter(200, nil, opts, testMessageWriterMetrics()).(*messageWriterImpl)

	nowNanos := time.Now().UnixNano()
	m := newMessage()
	m.IncWriteTimes()
	retryAtNanos := w.nextRetryNanos(m.WriteTimes(), nowNanos)
	require.True(t, retryAtNanos > nowNanos)
	require.True(t, retryAtNanos < nowNanos+int64(backoffDuration))

	m.IncWriteTimes()
	retryAtNanos = w.nextRetryNanos(m.WriteTimes(), nowNanos)
	require.True(t, retryAtNanos >= nowNanos+int64(backoffDuration))
	require.True(t, retryAtNanos < nowNanos+2*int64(backoffDuration))

	m.IncWriteTimes()
	retryAtNanos = w.nextRetryNanos(m.WriteTimes(), nowNanos)
	require.True(t, retryAtNanos == nowNanos+2*int64(backoffDuration))
}

func TestMessageWriterCloseCleanupAllMessages(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newMessageWriter(200, nil, opts, testMessageWriterMetrics()).(*messageWriterImpl)

	ctrl := gomock.NewController(t)
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
	return newMessageWriterMetrics(tally.NoopScope, 1)
}
