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

package buffer

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3x/retry"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestOptionsValidation(t *testing.T) {
	opts := NewOptions()
	require.NoError(t, opts.Validate())

	opts = opts.SetMaxMessageSize(100).SetMaxBufferSize(1)
	require.Equal(t, errInvalidMaxMessageSize, opts.Validate())

	opts = opts.SetMaxMessageSize(-1)
	require.Equal(t, errNegativeMaxMessageSize, opts.Validate())

	opts = opts.SetMaxBufferSize(-1)
	require.Equal(t, errNegativeMaxBufferSize, opts.Validate())

	opts = opts.SetScanBatchSize(0)
	require.Equal(t, errInvalidScanBatchSize, opts.Validate())
}

func TestBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, testOptions())
	require.Equal(t, 0, int(b.size.Load()))
	require.Equal(t, 0, b.bufferList.Len())

	rm, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, mm.Size(), int(b.size.Load()))

	mm.EXPECT().Finalize(producer.Consumed)
	// Finalize the message will reduce the buffer size.
	rm.IncRef()
	rm.DecRef()
	require.Equal(t, 0, int(b.size.Load()))
}

func TestBufferAddMessageTooLarge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, NewOptions().SetMaxMessageSize(1))
	_, err := b.Add(mm)
	require.Error(t, err)
	require.Equal(t, errMessageTooLarge, err)
}

func TestBufferAddMessageLargerThanMaxBufferSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, NewOptions().
		SetMaxMessageSize(1).
		SetMaxBufferSize(1),
	)
	_, err := b.Add(mm)
	require.Error(t, err)
	require.Equal(t, errMessageTooLarge, err)
}

func TestBufferCleanupOldest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, NewOptions())
	rm, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, int(rm.Size()), mm.Size())
	require.Equal(t, rm.Size(), b.size.Load())
	require.Equal(t, 1, b.bufferList.Len())

	mm.EXPECT().Finalize(producer.Dropped)
	b.dropOldestUntilTarget(0)
	require.Equal(t, uint64(0), b.size.Load())
	require.Equal(t, 0, b.bufferList.Len())
}

func TestBufferCleanupOldestBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, NewOptions())
	_, err := b.Add(mm)
	require.NoError(t, err)
	_, err = b.Add(mm)
	require.NoError(t, err)
	_, err = b.Add(mm)
	require.NoError(t, err)
	_, err = b.Add(mm)
	require.NoError(t, err)
	_, err = b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 5*mm.Size(), int(b.size.Load()))
	require.Equal(t, 5, b.bufferList.Len())

	mm.EXPECT().Finalize(producer.Dropped)
	// Didn't need to iterate through the full batch size to get to target size.
	shouldContinue := b.dropOldestBatchUntilTargetWithListLock(uint64(450), 2)
	require.False(t, shouldContinue)
	require.Equal(t, uint64(4*mm.Size()), b.size.Load())
	require.Equal(t, 4, b.bufferList.Len())

	mm.EXPECT().Finalize(producer.Dropped).Times(2)
	shouldContinue = b.dropOldestBatchUntilTargetWithListLock(uint64(50), 2)
	require.True(t, shouldContinue)
	require.Equal(t, uint64(200), b.size.Load())
	require.Equal(t, 2, b.bufferList.Len())

	mm.EXPECT().Finalize(producer.Dropped).Times(2)
	b.dropOldestUntilTarget(0)
	require.Equal(t, uint64(0), b.size.Load())
	require.Equal(t, 0, b.bufferList.Len())
}

func TestCleanupBatch(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(1).AnyTimes()

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(2).AnyTimes()

	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(3).AnyTimes()

	b := mustNewBuffer(t, NewOptions().SetScanBatchSize(2))
	_, err := b.Add(mm1)
	require.NoError(t, err)
	_, err = b.Add(mm2)
	require.NoError(t, err)
	_, err = b.Add(mm3)
	require.NoError(t, err)

	mm1.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	front := b.bufferList.Front()
	front.Value.(*producer.RefCountedMessage).Drop()

	require.Equal(t, 3, b.bufferLen())
	e, removed := b.cleanupBatchWithListLock(front, 2, false)
	require.Equal(t, 3, int(e.Value.(*producer.RefCountedMessage).Size()))
	require.Equal(t, 2, b.bufferLen())
	require.Equal(t, 1, removed)

	e, removed = b.cleanupBatchWithListLock(e, 2, false)
	require.Nil(t, e)
	require.Equal(t, 2, b.bufferLen())
	require.Equal(t, 0, removed)
}

func TestCleanupBatchWithElementBeingRemovedByOtherThread(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(1).AnyTimes()

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(2).AnyTimes()

	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(3).AnyTimes()

	b := mustNewBuffer(t, NewOptions())
	_, err := b.Add(mm1)
	require.NoError(t, err)
	_, err = b.Add(mm2)
	require.NoError(t, err)
	_, err = b.Add(mm3)
	require.NoError(t, err)
	require.Error(t, b.cleanup())

	mm1.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	b.bufferList.Front().Value.(*producer.RefCountedMessage).Drop()

	require.Equal(t, 3, b.bufferLen())
	e, removed := b.cleanupBatchWithListLock(b.bufferList.Front(), 1, false)
	// e stopped at message 2.
	require.Equal(t, 2, int(e.Value.(*producer.RefCountedMessage).Size()))
	require.Equal(t, 2, b.bufferLen())
	require.Equal(t, 1, removed)
	require.NotNil(t, e)

	require.NotNil(t, e.Next())
	// Mimic A new write triggered DropOldest and removed message 2.
	b.bufferList.Remove(e)
	require.Nil(t, e.Next())
	require.Equal(t, 1, b.bufferLen())

	// Mark message 3 as dropped, so it's ready to be removed.
	mm3.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	b.bufferList.Front().Value.(*producer.RefCountedMessage).Drop()

	// But next clean batch from the removed element is going to do nothing
	// because the starting element is already removed.
	e, removed = b.cleanupBatchWithListLock(e, 3, false)
	require.Equal(t, 1, b.bufferLen())
	require.Equal(t, 0, removed)
	require.Nil(t, e)

	// Next tick will start from the beginning again and will remove
	// the dropped message.
	require.NoError(t, b.cleanup())
	require.Equal(t, 0, b.bufferLen())
}

func TestBufferCleanupBackground(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, testOptions())
	rm, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, rm.Size(), uint64(mm.Size()))
	require.Equal(t, rm.Size(), b.size.Load())

	b.Init()
	mm.EXPECT().Finalize(producer.Consumed)
	rm.IncRef()
	rm.DecRef()

	b.Close(producer.WaitForConsumption)
	require.Equal(t, 0, int(b.size.Load()))
	_, err = b.Add(mm)
	require.Error(t, err)
	// Safe to close again.
	b.Close(producer.WaitForConsumption)
}

func TestListRemoveCleanupNextAndPrev(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, NewOptions())
	_, err := b.Add(mm)
	require.NoError(t, err)
	_, err = b.Add(mm)
	require.NoError(t, err)
	_, err = b.Add(mm)
	require.NoError(t, err)

	e := b.bufferList.Front().Next()
	require.NotNil(t, e.Next())
	require.NotNil(t, e.Prev())

	b.bufferList.Remove(e)
	require.Nil(t, e.Next())
	require.Nil(t, e.Prev())
}

func TestBufferCloseDropEverything(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, testOptions())
	rm, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, rm.Size(), uint64(mm.Size()))
	require.Equal(t, rm.Size(), b.size.Load())

	b.Init()
	mm.EXPECT().Finalize(producer.Dropped)
	b.Close(producer.DropEverything)
	for {
		l := b.size.Load()
		if l == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestBufferDropOldestAsyncOnFull(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t,
		testOptions().
			SetAllowedSpilloverRatio(0.8).
			SetMaxMessageSize(3*int(mm.Size())).
			SetMaxBufferSize(3*int(mm.Size())),
	)
	require.Equal(t, 540, int(b.maxSpilloverSize))
	rd1, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 100, int(b.size.Load()))
	rd2, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 200, int(b.size.Load()))
	rd3, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 300, int(b.size.Load()))
	require.False(t, rd1.IsDroppedOrConsumed())
	require.False(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())
	require.Equal(t, b.maxBufferSize, b.size.Load())

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(2 * mm.Size()).AnyTimes()

	require.Equal(t, 0, len(b.dropOldestCh))
	_, err = b.Add(mm2)
	require.NoError(t, err)
	require.Equal(t, 500, int(b.size.Load()))
	require.Equal(t, 1, len(b.dropOldestCh))

	var wg sync.WaitGroup
	wg.Add(2)
	mm.EXPECT().Finalize(producer.Dropped).Do(func(interface{}) {
		wg.Done()
	}).Times(2)
	b.Init()
	wg.Wait()
	require.True(t, rd1.IsDroppedOrConsumed())
	require.True(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())

	mm.EXPECT().Finalize(producer.Dropped)
	mm2.EXPECT().Finalize(producer.Dropped)
	b.Close(producer.DropEverything)
	require.Equal(t, 0, int(b.size.Load()))
}

func TestBufferDropOldestsyncOnFull(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t,
		testOptions().
			SetAllowedSpilloverRatio(0.2).
			SetMaxMessageSize(3*int(mm.Size())).
			SetMaxBufferSize(3*int(mm.Size())),
	)
	require.Equal(t, 360, int(b.maxSpilloverSize))
	rd1, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 100, int(b.size.Load()))
	rd2, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 200, int(b.size.Load()))
	rd3, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 300, int(b.size.Load()))
	require.False(t, rd1.IsDroppedOrConsumed())
	require.False(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())
	require.Equal(t, b.maxBufferSize, b.size.Load())

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(2 * mm.Size()).AnyTimes()

	require.Equal(t, 0, len(b.dropOldestCh))
	mm.EXPECT().Finalize(producer.Dropped).Times(2)
	// This will trigger dropEarlist synchronizely as it reached
	// max allowed spill over ratio.
	_, err = b.Add(mm2)
	require.NoError(t, err)
	require.Equal(t, 300, int(b.size.Load()))
	require.Equal(t, 0, len(b.dropOldestCh))
}

func TestBufferReturnErrorOnFull(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()

	b := mustNewBuffer(t, testOptions().
		SetMaxMessageSize(int(mm.Size())).
		SetMaxBufferSize(3*int(mm.Size())).
		SetOnFullStrategy(ReturnError),
	)

	rd1, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 100, int(b.size.Load()))
	rd2, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 200, int(b.size.Load()))
	rd3, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, 300, int(b.size.Load()))
	require.False(t, rd1.IsDroppedOrConsumed())
	require.False(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())

	_, err = b.Add(mm)
	require.Error(t, err)
	require.Equal(t, 300, int(b.size.Load()))
}

func mustNewBuffer(t testing.TB, opts Options) *buffer {
	b, err := NewBuffer(opts)
	require.NoError(t, err)
	return b.(*buffer)
}

func testOptions() Options {
	return NewOptions().
		SetCloseCheckInterval(100 * time.Millisecond).
		SetDropOldestInterval(100 * time.Millisecond).
		SetCleanupRetryOptions(retry.NewOptions().SetInitialBackoff(100 * time.Millisecond).SetMaxBackoff(200 * time.Millisecond))
}

func BenchmarkProduce(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(100).AnyTimes()
	mm.EXPECT().Finalize(producer.Dropped).AnyTimes()

	buffer := mustNewBuffer(b, NewOptions().
		SetMaxBufferSize(1000*1000*200).
		SetDropOldestInterval(100*time.Millisecond).
		SetOnFullStrategy(DropOldest),
	)

	for n := 0; n < b.N; n++ {
		_, err := buffer.Add(mm)
		if err != nil {
			b.FailNow()
		}
	}
}
