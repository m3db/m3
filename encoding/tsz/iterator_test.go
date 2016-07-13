// Copyright (c) 2016 Uber Technologies, Inc.
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

package tsz

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/require"
)

func getTestReaderIterator(rawBytes []byte) *readerIterator {
	return NewReaderIterator(bytes.NewReader(rawBytes), NewOptions()).(*readerIterator)
}

func TestReaderIteratorReadNextTimestamp(t *testing.T) {
	inputs := []struct {
		previousTimeDelta time.Duration
		timeUnit          xtime.Unit
		rawBytes          []byte
		expectedTimeDelta time.Duration
	}{
		{62 * time.Second, xtime.Second, []byte{0x0}, 62 * time.Second},
		{65 * time.Second, xtime.Second, []byte{0xa0, 0x0}, time.Second},
		{65 * time.Second, xtime.Second, []byte{0x90, 0x0}, 97 * time.Second},
		{65 * time.Second, xtime.Second, []byte{0xd0, 0x0}, -191 * time.Second},
		{65 * time.Second, xtime.Second, []byte{0xcf, 0xf0}, 320 * time.Second},
		{65 * time.Second, xtime.Second, []byte{0xe8, 0x0}, -1983 * time.Second},
		{65 * time.Second, xtime.Second, []byte{0xe7, 0xff}, 2112 * time.Second},
		{65 * time.Second, xtime.Second, []byte{0xf0, 0x0, 0x1, 0x0, 0x0}, 4161 * time.Second},
		{65 * time.Second, xtime.Second, []byte{0xff, 0xff, 0xff, 0x0, 0x0}, -4031 * time.Second},
		{65 * time.Second, xtime.Nanosecond, []byte{0xff, 0xff, 0xff, 0xc4, 0x65, 0x36, 0x0, 0x0, 0x0}, -4031 * time.Second},
		{65 * time.Second, xtime.Second, []byte{0x80, 0x40, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7d, 0x0}, 65000001 * time.Microsecond},
	}

	for _, input := range inputs {
		it := getTestReaderIterator(input.rawBytes)
		it.tu = input.timeUnit
		it.dt = input.previousTimeDelta
		it.readNextTimestamp()
		require.Equal(t, input.expectedTimeDelta, it.dt)
		require.NoError(t, it.Err())
	}

	it := getTestReaderIterator([]byte{0x1})
	it.readNextTimestamp()
	require.Error(t, it.Err())
	it.readNextTimestamp()
	require.Error(t, it.Err())
}

func TestReaderIteratorReadNextValue(t *testing.T) {
	inputs := []struct {
		previousValue    uint64
		previousValueXOR uint64
		rawBytes         []byte
		expectedValueXOR uint64
		expectedValue    uint64
	}{
		{0x1234, 0x4028000000000000, []byte{0x0}, 0x0, 0x1234},
		{0xaaaaaa, 0x4028000000000000, []byte{0x80, 0x90}, 0x0120000000000000, 0x0120000000aaaaaa},
		{0xdeadbeef, 0x0120000000000000, []byte{0xc1, 0x2e, 0x1, 0x40}, 0x4028000000000000, 0x40280000deadbeef},
	}
	for _, input := range inputs {
		it := getTestReaderIterator(input.rawBytes)
		it.vb = input.previousValue
		it.xor = input.previousValueXOR
		it.readNextValue()
		require.Equal(t, input.expectedValueXOR, it.xor)
		require.Equal(t, input.expectedValue, it.vb)
		require.NoError(t, it.Err())
	}

	it := getTestReaderIterator([]byte{0xf0})
	it.readNextValue()
	require.Error(t, it.Err())
}

func TestReaderIteratorReadAnnotation(t *testing.T) {
	inputs := []struct {
		rawBytes           []byte
		expectedAnnotation m3db.Annotation
	}{
		{
			[]byte{0x0, 0xff},
			[]byte{0xff},
		},
		{
			[]byte{0x2, 0x2, 0x3},
			[]byte{0x2, 0x3},
		},
		{
			[]byte{0xe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
		{
			[]byte{0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}
	for _, input := range inputs {
		it := getTestReaderIterator(input.rawBytes)
		it.readAnnotation()
		require.Equal(t, input.expectedAnnotation, it.ant)
	}
}

func TestReaderIteratorReadTimeUnit(t *testing.T) {
	inputs := []struct {
		timeUnit                xtime.Unit
		rawBytes                []byte
		expectedTimeUnit        xtime.Unit
		expectedTimeUnitChanged bool
	}{
		{
			xtime.Millisecond,
			[]byte{0x1},
			xtime.Second,
			true,
		},
		{
			xtime.Second,
			[]byte{0x0},
			xtime.None,
			false,
		},
	}
	for _, input := range inputs {
		it := getTestReaderIterator(input.rawBytes)
		it.tu = input.timeUnit
		it.readTimeUnit()
		require.NoError(t, it.err)
		require.Equal(t, input.expectedTimeUnit, it.tu)
		require.Equal(t, input.expectedTimeUnitChanged, it.tuChanged)
	}
}

func TestReaderIteratorNextNoAnnotation(t *testing.T) {
	rawBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x9f, 0x20, 0x14, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x5f, 0x8c, 0xb0, 0x3a, 0x0, 0xe1, 0x0, 0x78, 0x0, 0x0,
		0x40, 0x6, 0x58, 0x76, 0x8e, 0x0, 0x0,
	}
	startTime := time.Unix(1427162462, 0)
	inputs := []m3db.Datapoint{
		{startTime, 12},
		{startTime.Add(time.Second * 60), 12},
		{startTime.Add(time.Second * 120), 24},
		{startTime.Add(-time.Second * 76), 24},
		{startTime.Add(-time.Second * 16), 24},
		{startTime.Add(time.Second * 2092), 15},
		{startTime.Add(time.Second * 4200), 12},
	}
	it := getTestReaderIterator(rawBytes)
	for i := 0; i < len(inputs); i++ {
		require.True(t, it.Next())
		v, u, a := it.Current()
		require.Nil(t, a)
		require.Equal(t, inputs[i].Timestamp, v.Timestamp)
		require.Equal(t, inputs[i].Value, v.Value)
		require.Equal(t, xtime.Second, u)
		require.NoError(t, it.Err())
		require.False(t, it.hasError())
		require.False(t, it.isDone())
	}

	for i := 0; i < 2; i++ {
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.False(t, it.hasError())
		require.True(t, it.isDone())
	}

	it = getTestReaderIterator([]byte{0xf0})
	it.readNextValue()
	require.False(t, it.Next())
	require.False(t, it.isDone())
	require.True(t, it.hasError())
}

func TestReaderIteratorNextWithAnnotation(t *testing.T) {
	rawBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x20, 0x1, 0x53, 0xe4,
		0x2, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x7, 0x40, 0x10, 0x4,
		0x8, 0x4, 0xb, 0x84, 0x1, 0xe0, 0x0, 0x1, 0x0, 0x19, 0x61, 0xda, 0x38, 0x0,
	}
	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp  m3db.Datapoint
		ant m3db.Annotation
	}{
		{m3db.Datapoint{startTime, 12}, []byte{0xa}},
		{m3db.Datapoint{startTime.Add(time.Second * 60), 12}, nil},
		{m3db.Datapoint{startTime.Add(time.Second * 120), 24}, nil},
		{m3db.Datapoint{startTime.Add(-time.Second * 76), 24}, nil},
		{m3db.Datapoint{startTime.Add(-time.Second * 16), 24}, []byte{0x1, 0x2}},
		{m3db.Datapoint{startTime.Add(time.Second * 2092), 15}, nil},
		{m3db.Datapoint{startTime.Add(time.Second * 4200), 12}, nil},
	}
	it := getTestReaderIterator(rawBytes)
	for i := 0; i < len(inputs); i++ {
		require.True(t, it.Next())
		v, u, a := it.Current()
		require.Equal(t, inputs[i].ant, a)
		require.Equal(t, inputs[i].dp.Timestamp, v.Timestamp)
		require.Equal(t, inputs[i].dp.Value, v.Value)
		require.Equal(t, xtime.Second, u)
		require.NoError(t, it.Err())
		require.False(t, it.hasError())
		require.False(t, it.isDone())
	}

	for i := 0; i < 2; i++ {
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.False(t, it.hasError())
		require.True(t, it.isDone())
	}

	it = getTestReaderIterator(
		[]byte{0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x20, 0x1, 0x50, 0x8},
	)
	require.False(t, it.Next())
	require.False(t, it.isDone())
	require.True(t, it.hasError())
}

func TestReaderIteratorNextWithTimeUnit(t *testing.T) {
	rawBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x9f, 0x20, 0x14, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x5f, 0x8c, 0xb0, 0x3a, 0x0, 0xe1, 0x0, 0x40, 0x20,
		0x4f, 0xff, 0xff, 0xff, 0x22, 0x58, 0x60, 0xd0, 0xc, 0xb0, 0xee, 0x1, 0x1,
		0x0, 0x0, 0x0, 0x1, 0xa4, 0x36, 0x76, 0x80, 0x47, 0x0, 0x80, 0x7f, 0xff,
		0xff, 0xff, 0x7f, 0xd9, 0x9a, 0x80, 0x11, 0x44, 0x0,
	}
	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp m3db.Datapoint
		tu xtime.Unit
	}{
		{m3db.Datapoint{startTime, 12}, xtime.Second},
		{m3db.Datapoint{startTime.Add(time.Second * 60), 12}, xtime.Second},
		{m3db.Datapoint{startTime.Add(time.Second * 120), 24}, xtime.Second},
		{m3db.Datapoint{startTime.Add(-time.Second * 76), 24}, xtime.Second},
		{m3db.Datapoint{startTime.Add(-time.Second * 16), 24}, xtime.Second},
		{m3db.Datapoint{startTime.Add(-time.Nanosecond * 15500000000), 15}, xtime.Nanosecond},
		{m3db.Datapoint{startTime.Add(-time.Millisecond * 1400), 12}, xtime.Millisecond},
		{m3db.Datapoint{startTime.Add(-time.Second * 10), 12}, xtime.Second},
		{m3db.Datapoint{startTime.Add(time.Second * 10), 12}, xtime.Second},
	}
	it := getTestReaderIterator(rawBytes)
	for i := 0; i < len(inputs); i++ {
		require.True(t, it.Next())
		v, u, _ := it.Current()
		require.Equal(t, inputs[i].dp.Timestamp, v.Timestamp)
		require.Equal(t, inputs[i].dp.Value, v.Value)
		require.Equal(t, inputs[i].tu, u)

		require.NoError(t, it.Err())
		require.False(t, it.hasError())
		require.False(t, it.isDone())
	}

	for i := 0; i < 2; i++ {
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.False(t, it.hasError())
		require.True(t, it.isDone())
	}
}

func TestReaderIteratorNextWithAnnotationAndTimeUnit(t *testing.T) {
	rawBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x20, 0x1, 0x53, 0xe4,
		0x2, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x6, 0x0, 0x81, 0x0,
		0x81, 0x68, 0x2, 0x1, 0x1, 0x0, 0x0, 0x0, 0x1d, 0xcd, 0x65, 0x0, 0x0, 0x20,
		0x8, 0x20, 0x18, 0x20, 0x2f, 0xf, 0xa6, 0x58, 0x77, 0x0, 0x80, 0x40, 0x0,
		0x0, 0x0, 0xe, 0xe6, 0xb2, 0x80, 0x23, 0x80, 0x0,
	}
	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp  m3db.Datapoint
		ant m3db.Annotation
		tu  xtime.Unit
	}{
		{m3db.Datapoint{startTime, 12}, []byte{0xa}, xtime.Second},
		{m3db.Datapoint{startTime.Add(time.Second * 60), 12}, nil, xtime.Second},
		{m3db.Datapoint{startTime.Add(time.Second * 120), 24}, nil, xtime.Second},
		{m3db.Datapoint{startTime.Add(-time.Second * 76), 24}, []byte{0x1, 0x2}, xtime.Second},
		{m3db.Datapoint{startTime.Add(-time.Second * 16), 24}, nil, xtime.Millisecond},
		{m3db.Datapoint{startTime.Add(-time.Millisecond * 15500), 15}, []byte{0x3, 0x4, 0x5}, xtime.Millisecond},
		{m3db.Datapoint{startTime.Add(-time.Millisecond * 14000), 12}, nil, xtime.Second},
	}
	it := getTestReaderIterator(rawBytes)
	for i := 0; i < len(inputs); i++ {
		require.True(t, it.Next())
		v, u, a := it.Current()
		require.Equal(t, inputs[i].ant, a)
		require.Equal(t, inputs[i].dp.Timestamp, v.Timestamp)
		require.Equal(t, inputs[i].dp.Value, v.Value)
		require.Equal(t, inputs[i].tu, u)

		require.NoError(t, it.Err())
		require.False(t, it.hasError())
		require.False(t, it.isDone())
	}

	for i := 0; i < 2; i++ {
		require.False(t, it.Next())
		require.NoError(t, it.Err())
		require.False(t, it.hasError())
		require.True(t, it.isDone())
	}
}

func TestReaderIteratorNextWithUnexpectedTimeUnit(t *testing.T) {
	rawBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x41, 0x20, 0x0, 0x0,
	}
	it := getTestReaderIterator(rawBytes)
	require.False(t, it.Next())
	expectedErr := errors.New("time encoding scheme for time unit 9 doesn't exist")
	require.Equal(t, expectedErr, it.Err())
}
