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

package msg

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3msg/producer"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRefCountedMessageConsume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()
	mm.EXPECT().Finalize(producer.Consumed)

	rm := NewRefCountedMessage(mm, nil)
	require.Equal(t, uint64(mm.Size()), rm.Size())
	require.False(t, rm.IsDroppedOrConsumed())

	rm.IncRef()
	rm.DecRef()
	require.True(t, rm.IsDroppedOrConsumed())

	rm.IncRef()
	rm.DecRef()
	require.True(t, rm.IsDroppedOrConsumed())

	rm.Drop()
	require.True(t, rm.IsDroppedOrConsumed())
}

func TestRefCountedMessageDrop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Size().Return(uint32(100)).AnyTimes()
	mm.EXPECT().Finalize(producer.Dropped)

	rm := NewRefCountedMessage(mm, nil)
	require.Equal(t, uint64(mm.Size()), rm.Size())
	require.False(t, rm.IsDroppedOrConsumed())

	rm.Drop()
	require.True(t, rm.IsDroppedOrConsumed())

	rm.IncRef()
	rm.DecRef()
	require.True(t, rm.IsDroppedOrConsumed())

	rm.Drop()
	require.True(t, rm.IsDroppedOrConsumed())
}

func TestRefCountedMessageBytesReadBlocking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mockBytes := []byte("foo")
	mm.EXPECT().Bytes().Return(mockBytes)

	rm := NewRefCountedMessage(mm, nil)
	rm.IncReads()
	b := rm.Bytes()
	require.Equal(t, mockBytes, b)
	require.False(t, rm.IsDroppedOrConsumed())

	doneCh := make(chan struct{})
	go func() {
		mm.EXPECT().Finalize(producer.Dropped)
		rm.Drop()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		require.FailNow(t, "not expected")
	case <-time.After(time.Second):
	}
	rm.DecReads()
	<-doneCh
	require.True(t, rm.IsDroppedOrConsumed())
}

func TestRefCountedMessageDecPanic(t *testing.T) {
	rm := NewRefCountedMessage(nil, nil)
	require.Panics(t, rm.DecRef)
}

func TestRefCountedMessageFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var called int
	filter := func(m producer.Message) bool {
		called++
		return m.Shard() == 0
	}

	mm := producer.NewMockMessage(ctrl)
	rm := NewRefCountedMessage(mm, nil)

	mm.EXPECT().Shard().Return(uint32(0))
	require.True(t, rm.Accept(filter))

	mm.EXPECT().Shard().Return(uint32(1))
	require.False(t, rm.Accept(filter))
}

func TestRefCountedMessageOnDropFn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Finalize(producer.Dropped)

	var called int
	fn := func(rm producer.RefCountedMessage) {
		called++
	}

	rm := NewRefCountedMessage(mm, fn)
	require.True(t, rm.Drop())
	require.Equal(t, 1, called)

	require.False(t, rm.Drop())
}

func TestRefCountedMessageNoBlocking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mm := producer.NewMockMessage(ctrl)
	for i := 0; i < 10000; i++ {
		rm := NewRefCountedMessage(mm, nil)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			rm.IncReads()
			rm.IsDroppedOrConsumed()
			rm.DecReads()
			wg.Done()
		}()
		go func() {
			mm.EXPECT().Finalize(producer.Dropped)
			rm.Drop()
			wg.Done()
		}()
		wg.Wait()
	}
}
