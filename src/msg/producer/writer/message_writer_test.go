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
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	xtest "github.com/m3db/m3/src/x/test"
)

func TestMessageWriter(t *testing.T) {
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

	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())
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

func TestMessageWriterRetry(t *testing.T) {
	defer leaktest.Check(t)()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	addr := lis.Addr().String()
	opts := testOptions()
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())
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

	require.Equal(t, 1, w.acks.size())
	w.acks.mtx.Lock()
	_, ok := w.acks.acks[uint64(1)]
	require.True(t, ok)
	w.acks.mtx.Unlock()

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

func TestMessageWriterCleanupDroppedMessage(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())

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
	m := w.mPool.Get()
	require.Nil(t, m.RefCountedMessage)

	require.Equal(t, 1, w.queue.Len())
	w.Init()
	defer w.Close()

	for {
		w.Lock()
		l := w.queue.Len()
		w.Unlock()
		if l != 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, isEmptyWithLock(w.acks))
}

func TestMessageWriterCleanupAckedMessage(t *testing.T) {
	defer leaktest.Check(t)()

	opts := testOptions()
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())
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
}

func TestMessageWriterCutoverCutoff(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	w := newMessageWriter(200, newMessagePool(), nil, testMessageWriterMetrics())
	now := time.Now()
	w.nowFn = func() time.Time { return now }
	met := w.Metrics()
	require.True(t, w.isValidWriteWithLock(now.UnixNano(), met))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+150, met))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+250, met))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+50, met))

	w.SetCutoffNanos(now.UnixNano() + 200)
	w.SetCutoverNanos(now.UnixNano() + 100)
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+150, met))
	require.False(t, w.isValidWriteWithLock(now.UnixNano()+250, met))
	require.False(t, w.isValidWriteWithLock(now.UnixNano()+50, met))
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

	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())
	now := time.Now()
	w.nowFn = func() time.Time { return now }

	w.SetCutoffNanos(now.UnixNano() + 200)
	w.SetCutoverNanos(now.UnixNano() + 100)
	met := w.Metrics()
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+150, met))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+250, met))
	require.True(t, w.isValidWriteWithLock(now.UnixNano()+50, met))
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

	opts := testOptions().SetMessageRetryNanosFn(
		NextRetryNanosFn(retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond)),
	)
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())

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
	opts := testOptions().SetMessageQueueScanBatchSize(retryBatchSize).SetMessageRetryNanosFn(
		NextRetryNanosFn(retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond)),
	)
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())

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
	opts := testOptions().SetMessageQueueScanBatchSize(retryBatchSize).SetMessageRetryNanosFn(
		NextRetryNanosFn(retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond)),
	)
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())

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
	opts := testOptions().SetMessageQueueScanBatchSize(retryBatchSize).SetMessageRetryNanosFn(
		NextRetryNanosFn(retry.NewOptions().SetInitialBackoff(2 * time.Nanosecond).SetMaxBackoff(5 * time.Nanosecond)),
	)
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())

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
	opts := testOptions().
		SetMessageRetryNanosFn(
			NextRetryNanosFn(
				retry.NewOptions().
					SetInitialBackoff(backoffDuration).
					SetMaxBackoff(2 * backoffDuration).
					SetJitter(true),
			),
		)
	w := newMessageWriter(200, nil, opts, testMessageWriterMetrics())

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

func TestStaticRetryAfterNanos(t *testing.T) {
	fn, err := StaticRetryNanosFn([]time.Duration{time.Minute, 10 * time.Second, 5 * time.Second})
	require.NoError(t, err)

	opts := testOptions().SetMessageRetryNanosFn(fn)
	w := newMessageWriter(200, nil, opts, testMessageWriterMetrics())

	m := newMessage()
	m.IncWriteTimes()
	retryAtNanos := w.nextRetryAfterNanos(m.WriteTimes())
	require.Equal(t, int64(time.Minute), retryAtNanos)

	m.IncWriteTimes()
	retryAtNanos = w.nextRetryAfterNanos(m.WriteTimes())
	require.Equal(t, int64(10*time.Second), retryAtNanos)

	m.IncWriteTimes()
	retryAtNanos = w.nextRetryAfterNanos(m.WriteTimes())
	require.Equal(t, int64(5*time.Second), retryAtNanos)

	m.IncWriteTimes()
	retryAtNanos = w.nextRetryAfterNanos(m.WriteTimes())
	require.Equal(t, int64(5*time.Second), retryAtNanos)
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
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())

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
	w := newMessageWriter(200, newMessagePool(), opts, metrics)
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
	w := newMessageWriter(200, nil, opts, metrics)
	w.AddConsumerWriter(newConsumerWriter("bad", nil, opts, testConsumerWriterMetrics()))

	snapshot := scope.Snapshot()
	counters := snapshot.Counters()
	require.Nil(t, counters["message-processed+consumer=c1,result=write"])
	require.NotNil(t, counters["message-processed+result=write"])
}

// processServerMessages handles reading messages from a connection and sending acknowledgments
// It can be configured to process messages at different speeds
func processServerMessages(
	t *testing.T,
	conn net.Conn,
	opts Options,
	processDelay time.Duration,
	processFn func(),
) {
	defer conn.Close() // nolint: errcheck

	// Process messages
	for {
		// Read message
		serverDecoder := proto.NewDecoder(conn, opts.DecoderOptions(), 10)
		var msg msgpb.Message
		err := serverDecoder.Decode(&msg)
		if err != nil {
			// Connection closed
			return
		}

		// Simulate processing delay if specified
		if processDelay > 0 {
			time.Sleep(processDelay)
		}

		processFn()

		// Send ack
		serverEncoder := proto.NewEncoder(opts.EncoderOptions())
		err = serverEncoder.Encode(&msgpb.Ack{
			Metadata: []msgpb.Metadata{
				msg.Metadata,
			},
		})
		require.NoError(t, err)
		_, err = conn.Write(serverEncoder.Bytes())
		require.NoError(t, err)
	}
}

func TestMessageWriterChooseConsumerWriter(t *testing.T) {
	defer leaktest.Check(t)()

	// Create a mock controller for message mocking
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Create a wait group to wait for server goroutines
	var wg sync.WaitGroup
	defer wg.Wait()

	// Create two listeners - one for fast server, one for slow server
	fastLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer fastLis.Close() // nolint: errcheck

	slowLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer slowLis.Close() // nolint: errcheck

	fastAddr := fastLis.Addr().String()
	slowAddr := slowLis.Addr().String()
	opts := testOptions()

	connOpts := opts.ConnectionOptions().SetWriteBufferSize(50)
	opts = opts.SetConnectionOptions(connOpts)
	// set scan interval low enough to ensure that the message writes are processed
	opts = opts.SetMessageQueueNewWritesScanInterval(2 * time.Millisecond)
	// set the retry interval long enough to ensure that
	// messages are not retried.
	opts = opts.SetMessageRetryNanosFn(
		NextRetryNanosFn(
			retry.NewOptions().
				SetInitialBackoff(500 * time.Millisecond).
				SetMaxBackoff(500 * time.Millisecond),
		),
	)

	// Create a messageWriter
	w := newMessageWriter(200, newMessagePool(), opts, testMessageWriterMetrics())
	require.Equal(t, 200, int(w.ReplicatedShardID()))
	w.Init()
	defer w.Close()

	// Create an ack router
	a := newAckRouter(1)
	a.Register(200, w)

	// Create two consumer writers
	fastCW := newConsumerWriter(fastAddr, a, opts, testConsumerWriterMetrics())
	fastCW.Init()
	defer fastCW.Close() // nolint:errcheck

	slowCW := newConsumerWriter(slowAddr, a, opts, testConsumerWriterMetrics())
	slowCW.Init()
	defer slowCW.Close()

	// Add both consumer writers to the message writer
	w.AddConsumerWriter(slowCW)
	w.AddConsumerWriter(fastCW)

	// Start the fast server
	slowCWMsgs := 0
	fastCWMsgs := 0
	slowCWFn := func() {
		slowCWMsgs++
	}
	fastCWFn := func() {
		fastCWMsgs++
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := fastLis.Accept()
		require.NoError(t, err)
		processServerMessages(t, conn, opts, 0, fastCWFn) // No delay for fast server
	}()

	// Start the slow server
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := slowLis.Accept()
		require.NoError(t, err)
		processServerMessages(t, conn, opts, 100*time.Millisecond, slowCWFn) // 100ms delay for slow server
	}()

	// Create and write multiple messages
	numMessages := 2

	for i := range numMessages {
		mm := producer.NewMockMessage(ctrl)
		mm.EXPECT().Bytes().Return([]byte(fmt.Sprintf("message-%d", i))).AnyTimes()
		mm.EXPECT().Size().Return(10).AnyTimes()
		mm.EXPECT().Finalize(producer.Consumed).AnyTimes()

		rm := producer.NewRefCountedMessage(mm, nil)
		w.Write(rm)
	}

	// Wait for message to be processed
	for {
		w.RLock()
		l := w.queue.Len()
		w.RUnlock()
		if l == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// We are sending 2 messages.
	// If the slow CW receives the first message then the next message
	// should be sent to the fast CW.
	// If the fast CW receives the first message then the next message should
	// still be sent to the fast CW.
	// Therefore, the fast CW should see at least 1 message.
	require.True(t, fastCWMsgs >= slowCWMsgs,
		"Fast consumer writer should be used more often. Fast: %d, Slow: %d",
		fastCWMsgs, slowCWMsgs)
}

func TestMessageWriterForcedFlush(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := testOptions()
	scope := tally.NewTestScope("", nil)
	metrics := newMessageWriterMetrics(scope, instrument.TimerOptions{}, true)
	w := newMessageWriter(200, nil, opts, metrics)
	slowConsumerWriter := NewMockconsumerWriter(ctrl)
	slowConsumerWriter.EXPECT().Address().Return("slow").AnyTimes()
	slowConsumerWriter.EXPECT().AvailableBuffer(gomock.Any()).Return(1).AnyTimes()
	slowConsumerWriter.EXPECT().ForcedFlush(gomock.Any()).DoAndReturn(func(_ int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}).AnyTimes()
	fastConsumerWriter := NewMockconsumerWriter(ctrl)
	fastConsumerWriter.EXPECT().Address().Return("fast").AnyTimes()
	fastConsumerWriter.EXPECT().AvailableBuffer(gomock.Any()).Return(10).AnyTimes()
	fastConsumerWriter.EXPECT().ForcedFlush(gomock.Any()).Return(nil).Times(1)
	w.AddConsumerWriter(slowConsumerWriter)
	w.AddConsumerWriter(fastConsumerWriter)

	cw := w.chooseConsumerWriter([]consumerWriter{slowConsumerWriter, fastConsumerWriter}, 0, 100)
	require.Equal(t, fastConsumerWriter.Address(), cw.Address())
}

func TestMessageWriterBeginAndWaitForForcedFlush(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := testOptions()
	scope := tally.NewTestScope("", nil)
	metrics := newMessageWriterMetrics(scope, instrument.TimerOptions{}, true)
	w := newMessageWriter(200, nil, opts, metrics)
	slowConsumerWriter := NewMockconsumerWriter(ctrl)
	slowConsumerWriter.EXPECT().Address().Return("slow").AnyTimes()
	slowConsumerWriter.EXPECT().AvailableBuffer(gomock.Any()).Return(1).AnyTimes()
	slowConsumerWriter.EXPECT().ForcedFlush(gomock.Any()).DoAndReturn(func(_ int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}).AnyTimes()
	fastConsumerWriter := NewMockconsumerWriter(ctrl)
	fastConsumerWriter.EXPECT().Address().Return("fast").AnyTimes()
	fastConsumerWriter.EXPECT().AvailableBuffer(gomock.Any()).Return(10).AnyTimes()
	fastConsumerWriter.EXPECT().ForcedFlush(gomock.Any()).Return(nil).Times(1)
	w.AddConsumerWriter(slowConsumerWriter)
	w.AddConsumerWriter(fastConsumerWriter)

	consumerWriters := []consumerWriter{slowConsumerWriter, fastConsumerWriter}
	doneCh := make(chan int, 2)
	w.beginForcedFlush(doneCh, consumerWriters, 0)
	cw := w.waitForForcedFlush(doneCh, consumerWriters)
	require.Equal(t, fastConsumerWriter.Address(), cw.Address())
}

func TestMessageWriterForcedFlushTimeout(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := testOptions()
	connOpts := opts.ConnectionOptions()
	opts = opts.SetConnectionOptions(connOpts.SetForcedFlushTimeout(10 * time.Millisecond))

	scope := tally.NewTestScope("", nil)
	metrics := newMessageWriterMetrics(scope, instrument.TimerOptions{}, true)
	w := newMessageWriter(200, nil, opts, metrics)
	slowConsumerWriter1 := NewMockconsumerWriter(ctrl)
	slowConsumerWriter1.EXPECT().Address().Return("slow1").AnyTimes()
	slowConsumerWriter1.EXPECT().AvailableBuffer(gomock.Any()).Return(1).AnyTimes()
	slowConsumerWriter1.EXPECT().ForcedFlush(gomock.Any()).DoAndReturn(func(_ int) error {
		time.Sleep(time.Second)
		return nil
	}).AnyTimes()
	slowConsumerWriter2 := NewMockconsumerWriter(ctrl)
	slowConsumerWriter2.EXPECT().Address().Return("slow2").AnyTimes()
	slowConsumerWriter2.EXPECT().AvailableBuffer(gomock.Any()).Return(10).AnyTimes()
	slowConsumerWriter2.EXPECT().ForcedFlush(gomock.Any()).DoAndReturn(func(_ int) error {
		time.Sleep(time.Second)
		return nil
	}).AnyTimes()
	w.AddConsumerWriter(slowConsumerWriter1)
	w.AddConsumerWriter(slowConsumerWriter2)

	consumerWriters := []consumerWriter{slowConsumerWriter1, slowConsumerWriter2}
	doneCh := make(chan int, 2)
	w.beginForcedFlush(doneCh, consumerWriters, 0)
	cw := w.waitForForcedFlush(doneCh, consumerWriters)
	require.Equal(t, nil, cw)
}

func TestMessageWriterGetConsumerWriterWithMaxBuffer(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := testOptions()
	scope := tally.NewTestScope("", nil)
	metrics := newMessageWriterMetrics(scope, instrument.TimerOptions{}, true)
	w := newMessageWriter(200, nil, opts, metrics)

	numConsumerWriters := 100
	cws := make([]consumerWriter, 0, numConsumerWriters)
	for i := range numConsumerWriters {
		cw := NewMockconsumerWriter(ctrl)
		cw.EXPECT().Address().Return(fmt.Sprintf("addr-%d", i)).AnyTimes()
		cw.EXPECT().AvailableBuffer(gomock.Any()).Return(i).AnyTimes()
		w.AddConsumerWriter(cw)
		cws = append(cws, cw)
	}

	rand.Shuffle(len(cws), func(i, j int) {
		cws[i], cws[j] = cws[j], cws[i]
	})

	cw, maxBuf := w.getConsumerWriterWithMaxBuffer(cws, 0)
	require.Equal(t, "addr-99", cw.Address())
	require.Equal(t, 99, maxBuf)
}

func isEmptyWithLock(h *acks) bool {
	return h.size() == 0
}

func testMessageWriterMetrics() *messageWriterMetrics {
	return newMessageWriterMetrics(tally.NoopScope, instrument.TimerOptions{}, false)
}

func testMessageWriterMetricsWithScope(scope tally.TestScope) *messageWriterMetrics {
	return newMessageWriterMetrics(scope, instrument.TimerOptions{}, false)
}

func validateMessages(t *testing.T, msgs []*producer.RefCountedMessage, w *messageWriter) {
	w.RLock()
	idx := 0
	for e := w.queue.Front(); e != nil; e = e.Next() {
		require.Equal(t, msgs[idx].Bytes(), e.Value.(*message).RefCountedMessage.Bytes())
		idx++
	}
	w.RUnlock()
	require.Equal(t, idx, len(msgs))
}
