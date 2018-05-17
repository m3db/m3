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

package data

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3msg/producer"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRefCountedDataConsume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()
	md.EXPECT().Finalize(producer.Consumed)

	rd := NewRefCountedData(md, nil)
	require.Equal(t, uint64(md.Size()), rd.Size())
	require.False(t, rd.IsDroppedOrConsumed())

	rd.IncRef()
	rd.DecRef()
	require.True(t, rd.IsDroppedOrConsumed())

	rd.IncRef()
	rd.DecRef()
	require.True(t, rd.IsDroppedOrConsumed())

	rd.Drop()
	require.True(t, rd.IsDroppedOrConsumed())
}

func TestRefCountedDataDrop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()
	md.EXPECT().Finalize(producer.Dropped)

	rd := NewRefCountedData(md, nil)
	require.Equal(t, uint64(md.Size()), rd.Size())
	require.False(t, rd.IsDroppedOrConsumed())

	rd.Drop()
	require.True(t, rd.IsDroppedOrConsumed())

	rd.IncRef()
	rd.DecRef()
	require.True(t, rd.IsDroppedOrConsumed())

	rd.Drop()
	require.True(t, rd.IsDroppedOrConsumed())
}

func TestRefCountedDataBytesReadBlocking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	mockBytes := []byte("foo")
	md.EXPECT().Bytes().Return(mockBytes)

	rd := NewRefCountedData(md, nil)
	rd.IncReads()
	b := rd.Bytes()
	require.Equal(t, mockBytes, b)
	require.False(t, rd.IsDroppedOrConsumed())

	doneCh := make(chan struct{})
	go func() {
		md.EXPECT().Finalize(producer.Dropped)
		rd.Drop()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		require.FailNow(t, "not expected")
	case <-time.After(time.Second):
	}
	rd.DecReads()
	<-doneCh
	require.True(t, rd.IsDroppedOrConsumed())
}

func TestRefCountedDataDecPanic(t *testing.T) {
	rd := NewRefCountedData(nil, nil)
	require.Panics(t, rd.DecRef)
}

func TestRefCountedDataFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var called int
	filter := func(data producer.Data) bool {
		called++
		return data.Shard() == 0
	}

	md := producer.NewMockData(ctrl)
	rd := NewRefCountedData(md, nil)

	md.EXPECT().Shard().Return(uint32(0))
	require.True(t, rd.Accept(filter))

	md.EXPECT().Shard().Return(uint32(1))
	require.False(t, rd.Accept(filter))
}

func TestRefCountedDataOnDropFn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Finalize(producer.Dropped)

	var called int
	fn := func(d producer.RefCountedData) {
		called++
	}

	rd := NewRefCountedData(md, fn)
	require.True(t, rd.Drop())
	require.Equal(t, 1, called)

	require.False(t, rd.Drop())
}

func TestRefCountedDataNoBlocking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	for i := 0; i < 10000; i++ {
		rd := NewRefCountedData(md, nil)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			rd.IncReads()
			rd.IsDroppedOrConsumed()
			rd.DecReads()
			wg.Done()
		}()
		go func() {
			md.EXPECT().Finalize(producer.Dropped)
			rd.Drop()
			wg.Done()
		}()
		wg.Wait()
	}
}
