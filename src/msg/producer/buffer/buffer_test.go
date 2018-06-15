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
	"testing"
	"time"

	"github.com/m3db/m3msg/producer"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(nil)
	require.Equal(t, 0, int(b.(*buffer).size.Load()))
	require.Equal(t, 0, b.(*buffer).buffers.Len())

	rm, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, uint64(mm.Size()), b.(*buffer).size.Load())

	mm.EXPECT().Finalize(producer.Consumed)
	// Finalize the message will reduce the buffer size.
	rm.IncRef()
	rm.DecRef()
	require.Equal(t, 0, int(b.(*buffer).size.Load()))
}

func TestBufferAddMessageTooLarge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewOptions().SetMaxMessageSize(1))
	_, err := b.Add(mm)
	require.Error(t, err)
	require.Equal(t, errMessageTooLarge, err)
}

func TestBufferAddMessageLargerThanMaxBufferSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewOptions().SetMaxBufferSize(1))
	_, err := b.Add(mm)
	require.Error(t, err)
	require.Equal(t, errMessageLargerThanMaxBufferSize, err)
}

func TestBufferCleanupEarliest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewOptions()).(*buffer)
	rm, err := b.Add(mm)
	require.NoError(t, err)
	require.Equal(t, rm.Size(), uint64(mm.Size()))
	require.Equal(t, rm.Size(), b.size.Load())
	require.Equal(t, 1, b.buffers.Len())

	mm.EXPECT().Finalize(producer.Dropped)
	b.dropEarliestUntilTargetWithLock(0)
	require.Equal(t, uint64(0), b.size.Load())
	require.Equal(t, 0, b.buffers.Len())
}

func TestCleanupBatch(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(uint32(1)).Times(2)

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(uint32(2))

	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(uint32(3)).Times(2)

	b := NewBuffer(NewOptions().SetScanBatchSize(2)).(*buffer)
	_, err := b.Add(mm1)
	require.NoError(t, err)
	_, err = b.Add(mm2)
	require.NoError(t, err)
	_, err = b.Add(mm3)
	require.NoError(t, err)

	mm1.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	front := b.buffers.Front()
	front.Value.(producer.RefCountedMessage).Drop()

	require.Equal(t, 3, b.bufferLen())
	e := b.cleanupBatchWithLock(front, 2)
	require.Equal(t, 3, int(e.Value.(producer.RefCountedMessage).Size()))
	require.Equal(t, 2, b.bufferLen())

	e = b.cleanupBatchWithLock(e, 2)
	require.Nil(t, e)
	require.Equal(t, 2, b.bufferLen())
}

func TestCleanupBatchWithElementBeingRemovedByOtherThread(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm1 := producer.NewMockMessage(ctrl)
	mm1.EXPECT().Size().Return(uint32(1)).AnyTimes()

	mm2 := producer.NewMockMessage(ctrl)
	mm2.EXPECT().Size().Return(uint32(2)).AnyTimes()

	mm3 := producer.NewMockMessage(ctrl)
	mm3.EXPECT().Size().Return(uint32(3)).AnyTimes()

	b := NewBuffer(NewOptions()).(*buffer)
	_, err := b.Add(mm1)
	require.NoError(t, err)
	_, err = b.Add(mm2)
	require.NoError(t, err)
	_, err = b.Add(mm3)
	require.NoError(t, err)

	mm1.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	b.buffers.Front().Value.(producer.RefCountedMessage).Drop()

	require.Equal(t, 3, b.bufferLen())
	e := b.cleanupBatchWithLock(b.buffers.Front(), 1)
	// e stopped at message 2.
	require.Equal(t, 2, int(e.Value.(producer.RefCountedMessage).Size()))
	require.Equal(t, 2, b.bufferLen())
	require.NotNil(t, e)

	require.NotNil(t, e.Next())
	// Mimic A new write triggered DropEarliest and removed message 2.
	b.buffers.Remove(e)
	require.Nil(t, e.Next())
	require.Equal(t, 1, b.bufferLen())

	// Mark message 3 as dropped, so it's ready to be removed.
	mm3.EXPECT().Finalize(gomock.Eq(producer.Dropped))
	b.buffers.Front().Value.(producer.RefCountedMessage).Drop()

	// But next clean batch from the removed element is going to do nothing
	// because the starting element is already removed.
	e = b.cleanupBatchWithLock(e, 1)
	require.Equal(t, 1, b.bufferLen())
	require.Nil(t, e)

	// Next tick will start from the beginning again and will remove
	// the dropped message.
	b.cleanup()
	require.Equal(t, 0, b.bufferLen())
}

func TestBufferCleanupBackground(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(
		NewOptions().
			SetCleanupInterval(100 * time.Millisecond).
			SetCloseCheckInterval(100 * time.Millisecond),
	).(*buffer)
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
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewOptions()).(*buffer)
	_, err := b.Add(mm)
	require.NoError(t, err)
	_, err = b.Add(mm)
	require.NoError(t, err)
	_, err = b.Add(mm)
	require.NoError(t, err)

	e := b.buffers.Front().Next()
	require.NotNil(t, e.Next())
	require.NotNil(t, e.Prev())

	b.buffers.Remove(e)
	require.Nil(t, e.Next())
	require.Nil(t, e.Prev())
}

func TestBufferCloseDropEverything(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(
		NewOptions().
			SetCleanupInterval(100 * time.Millisecond).
			SetCloseCheckInterval(100 * time.Millisecond),
	).(*buffer)
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

func TestBufferDropEarliestOnFull(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewOptions().SetMaxBufferSize(int(3 * mm.Size())))
	rd1, err := b.Add(mm)
	require.NoError(t, err)
	rd2, err := b.Add(mm)
	require.NoError(t, err)
	rd3, err := b.Add(mm)
	require.NoError(t, err)
	require.False(t, rd1.IsDroppedOrConsumed())
	require.False(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())

	md2 := producer.NewMockMessage(ctrl)
	md2.EXPECT().Size().Return(2 * mm.Size()).AnyTimes()

	mm.EXPECT().Finalize(producer.Dropped).Times(2)
	_, err = b.Add(md2)
	require.NoError(t, err)
	require.True(t, rd1.IsDroppedOrConsumed())
	require.True(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())
}

func TestBufferReturnErrorOnFull(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(
		NewOptions().
			SetMaxBufferSize(int(3 * mm.Size())).
			SetOnFullStrategy(ReturnError),
	)

	rd1, err := b.Add(mm)
	require.NoError(t, err)
	rd2, err := b.Add(mm)
	require.NoError(t, err)
	rd3, err := b.Add(mm)
	require.NoError(t, err)
	require.False(t, rd1.IsDroppedOrConsumed())
	require.False(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())

	_, err = b.Add(mm)
	require.Error(t, err)
}

func BenchmarkProduce(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()
	mm.EXPECT().Finalize(producer.Dropped).AnyTimes()

	buffer := NewBuffer(
		NewOptions().
			SetMaxBufferSize(1000 * 1000 * 200).
			SetOnFullStrategy(DropEarliest),
	)

	for n := 0; n < b.N; n++ {
		_, err := buffer.Add(mm)
		if err != nil {
			b.FailNow()
		}
	}
}
