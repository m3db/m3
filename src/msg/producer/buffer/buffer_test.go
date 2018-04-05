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
	"github.com/m3db/m3x/instrument"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(nil)
	require.Equal(t, 0, int(b.(*buffer).size.Load()))
	require.Equal(t, 0, b.(*buffer).buffers.Len())

	rd, err := b.Add(md)
	require.NoError(t, err)
	require.Equal(t, uint64(md.Size()), b.(*buffer).size.Load())

	md.EXPECT().Finalize(producer.Consumed)
	// Finalize the data will reduce the buffer size.
	rd.IncRef()
	rd.DecRef()
	require.Equal(t, 0, int(b.(*buffer).size.Load()))
}

func TestBufferWithSmallSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewBufferOptions().SetMaxBufferSize(1))
	_, err := b.Add(md)
	require.Error(t, err)
}

func TestBufferCleanupEarliest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewBufferOptions())
	rd, err := b.Add(md)
	require.NoError(t, err)
	require.Equal(t, rd.Size(), uint64(md.Size()))
	require.Equal(t, rd.Size(), b.(*buffer).size.Load())

	md.EXPECT().Finalize(producer.Dropped)
	b.(*buffer).dropEarliestUntilTargetWithLock(0)
	require.Equal(t, uint64(0), b.(*buffer).size.Load())
}

func TestBufferCleanupBackground(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewBufferOptions().
		SetCleanupInterval(100 * time.Millisecond).
		SetCloseCheckInterval(100 * time.Millisecond).
		SetInstrumentOptions(instrument.NewOptions())).(*buffer)
	rd, err := b.Add(md)
	require.NoError(t, err)
	require.Equal(t, rd.Size(), uint64(md.Size()))
	require.Equal(t, rd.Size(), b.size.Load())

	b.Init()
	md.EXPECT().Finalize(producer.Consumed)
	rd.IncRef()
	rd.DecRef()

	b.Close()
	require.Equal(t, 0, int(b.size.Load()))
	_, err = b.Add(md)
	require.Error(t, err)
	// Safe to close again.
	b.Close()
}

func TestBufferDropEarliestOnFull(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(NewBufferOptions().SetMaxBufferSize(int(3 * md.Size())))
	rd1, err := b.Add(md)
	require.NoError(t, err)
	rd2, err := b.Add(md)
	require.NoError(t, err)
	rd3, err := b.Add(md)
	require.NoError(t, err)
	require.False(t, rd1.IsDroppedOrConsumed())
	require.False(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())

	md2 := producer.NewMockData(ctrl)
	md2.EXPECT().Size().Return(2 * md.Size()).AnyTimes()

	md.EXPECT().Finalize(producer.Dropped).Times(2)
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

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()

	b := NewBuffer(
		NewBufferOptions().
			SetMaxBufferSize(int(3 * md.Size())).
			SetOnFullStrategy(ReturnError),
	)

	rd1, err := b.Add(md)
	require.NoError(t, err)
	rd2, err := b.Add(md)
	require.NoError(t, err)
	rd3, err := b.Add(md)
	require.NoError(t, err)
	require.False(t, rd1.IsDroppedOrConsumed())
	require.False(t, rd2.IsDroppedOrConsumed())
	require.False(t, rd3.IsDroppedOrConsumed())

	_, err = b.Add(md)
	require.Error(t, err)
}

func BenchmarkProduce(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	md := producer.NewMockData(ctrl)
	md.EXPECT().Size().Return(uint32(100)).AnyTimes()
	md.EXPECT().Finalize(producer.Dropped).AnyTimes()

	buffer := NewBuffer(
		NewBufferOptions().
			SetMaxBufferSize(1000 * 1000 * 200).
			SetOnFullStrategy(DropEarliest),
	)

	for n := 0; n < b.N; n++ {
		_, err := buffer.Add(md)
		if err != nil {
			b.FailNow()
		}
	}
}
