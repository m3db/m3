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
	"io"
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testStartTime = time.Unix(1427162400, 0)
)

func getTestEncoder(startTime time.Time) *encoder {
	return NewEncoder(startTime, nil, nil).(*encoder)
}

func TestWriteDeltaOfDeltaTimeUnitUnchanged(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	inputs := []struct {
		delta         time.Duration
		timeUnit      xtime.Unit
		expectedBytes []byte
		expectedPos   int
	}{
		{0, xtime.Second, []byte{0x0}, 1},
		{32 * time.Second, xtime.Second, []byte{0x90, 0x0}, 1},
		{-63 * time.Second, xtime.Second, []byte{0xa0, 0x80}, 1},
		{-128 * time.Second, xtime.Second, []byte{0xd8, 0x0}, 4},
		{255 * time.Second, xtime.Second, []byte{0xcf, 0xf0}, 4},
		{-2048 * time.Second, xtime.Second, []byte{0xe8, 0x0}, 8},
		{2047 * time.Second, xtime.Second, []byte{0xe7, 0xff}, 8},
		{4096 * time.Second, xtime.Second, []byte{0xf0, 0x0, 0x1, 0x0, 0x0}, 4},
		{-4096 * time.Second, xtime.Second, []byte{0xff, 0xff, 0xff, 0x0, 0x0}, 4},
		{4096 * time.Second, xtime.Nanosecond, []byte{0xf0, 0x0, 0x0, 0x3b, 0x9a, 0xca, 0x0, 0x0, 0x0}, 4},
		{-4096 * time.Second, xtime.Nanosecond, []byte{0xff, 0xff, 0xff, 0xc4, 0x65, 0x36, 0x0, 0x0, 0x0}, 4},
	}
	for _, input := range inputs {
		encoder.Reset(testStartTime, 0)
		encoder.writeDeltaOfDelta(0, input.delta, input.timeUnit)
		require.Equal(t, input.expectedBytes, encoder.os.rawBuffer)
		require.Equal(t, input.expectedPos, encoder.os.pos)
	}
}

func TestWriteDeltaOfDeltaTimeUnitChanged(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	inputs := []struct {
		delta         time.Duration
		timeUnit      xtime.Unit
		expectedBytes []byte
		expectedPos   int
	}{
		{0, xtime.Second, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, 8},
		{32 * time.Millisecond, xtime.Second, []byte{0x0, 0x0, 0x0, 0x0, 0x1, 0xe8, 0x48, 0x0}, 8},
		{-63 * time.Microsecond, xtime.Millisecond, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x9, 0xe8}, 8},
	}
	for _, input := range inputs {
		encoder.Reset(testStartTime, 0)
		encoder.tuChanged = true
		encoder.writeDeltaOfDelta(0, input.delta, input.timeUnit)
		require.Equal(t, input.expectedBytes, encoder.os.rawBuffer)
		require.Equal(t, input.expectedPos, encoder.os.pos)
	}
}

func TestWriteValue(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	inputs := []struct {
		previousXOR   uint64
		currentXOR    uint64
		expectedBytes []byte
		expectedPos   int
	}{
		{0x4028000000000000, 0, []byte{0x0}, 1},
		{0x4028000000000000, 0x0120000000000000, []byte{0x80, 0x90}, 6},
		{0x0120000000000000, 0x4028000000000000, []byte{0xc1, 0x2e, 0x1, 0x40}, 2},
	}
	for _, input := range inputs {
		encoder.Reset(testStartTime, 0)
		encoder.writeXOR(input.previousXOR, input.currentXOR)
		require.Equal(t, input.expectedBytes, encoder.os.rawBuffer)
		require.Equal(t, input.expectedPos, encoder.os.pos)
	}
}

func TestWriteAnnotation(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	inputs := []struct {
		annotation    m3db.Annotation
		expectedBytes []byte
		expectedPos   int
	}{
		{
			nil,
			[]byte{},
			0,
		},
		{
			[]byte{0x1, 0x2},
			[]byte{0x80, 0x20, 0x40, 0x20, 0x40},
			3,
		},

		{
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			[]byte{0x80, 0x21, 0xdf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xe0},
			3,
		},
	}
	for _, input := range inputs {
		encoder.Reset(testStartTime, 0)
		encoder.writeAnnotation(input.annotation)
		require.Equal(t, input.expectedBytes, encoder.os.rawBuffer)
		require.Equal(t, input.expectedPos, encoder.os.pos)
	}
}

func getBytes(t *testing.T, r io.Reader) []byte {
	if r == nil {
		return nil
	}
	var b [1000]byte
	n, err := r.Read(b[:])
	require.NoError(t, err)
	return b[:n]
}

func TestWriteTimeUnit(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	inputs := []struct {
		timeUnit      xtime.Unit
		expectedBytes []byte
		expectedPos   int
	}{
		{
			xtime.None,
			[]byte{},
			0,
		},
		{
			xtime.Second,
			[]byte{0x80, 0x40, 0x20},
			3,
		},
		{
			xtime.Unit(5),
			[]byte{},
			0,
		},
	}
	for _, input := range inputs {
		encoder.Reset(testStartTime, 0)
		encoder.writeTimeUnit(input.timeUnit)
		require.Equal(t, input.expectedBytes, encoder.os.rawBuffer)
		require.Equal(t, input.expectedPos, encoder.os.pos)
	}
}

func TestEncodeNoAnnotation(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	require.Nil(t, encoder.Stream())

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
	for _, input := range inputs {
		encoder.Encode(input, xtime.Second, nil)
	}

	expectedBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x40, 0x20, 0x0, 0x0,
		0x1, 0xcd, 0xef, 0x9d, 0x80, 0x8, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x13,
		0xc3, 0x2c, 0xe, 0x80, 0x38, 0x40, 0x1e, 0x0, 0x0, 0x10, 0x1, 0x96, 0x1d,
		0xa3, 0x80, 0x0,
	}
	require.Equal(t, expectedBytes, getBytes(t, encoder.Stream()))

	expectedBuffer := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x40, 0x20, 0x0, 0x0,
		0x1, 0xcd, 0xef, 0x9d, 0x80, 0x8, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x13,
		0xc3, 0x2c, 0xe, 0x80, 0x38, 0x40, 0x1e, 0x0, 0x0, 0x10, 0x1, 0x96, 0x1d,
		0xa3,
	}
	require.Equal(t, expectedBuffer, encoder.os.rawBuffer)
	require.Equal(t, 8, encoder.os.pos)
}

func TestEncodeWithAnnotation(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	require.Nil(t, encoder.Stream())

	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp  m3db.Datapoint
		ant m3db.Annotation
	}{
		{m3db.Datapoint{startTime, 12}, []byte{0xa}},
		{m3db.Datapoint{startTime.Add(time.Second * 60), 12}, []byte{0xa}},
		{m3db.Datapoint{startTime.Add(time.Second * 120), 24}, nil},
		{m3db.Datapoint{startTime.Add(-time.Second * 76), 24}, nil},
		{m3db.Datapoint{startTime.Add(-time.Second * 16), 24}, []byte{0x1, 0x2}},
		{m3db.Datapoint{startTime.Add(time.Second * 2092), 15}, nil},
		{m3db.Datapoint{startTime.Add(time.Second * 4200), 12}, nil},
	}

	for _, input := range inputs {
		encoder.Encode(input.dp, xtime.Second, input.ant)
	}

	expectedBuffer := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x20, 0x1, 0x50, 0x8,
		0x4, 0x0, 0x0, 0x0, 0x39, 0xbd, 0xf3, 0xb0, 0x1, 0x0, 0xa0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x2, 0x78, 0x65, 0x81, 0xd0, 0x4, 0x1, 0x2, 0x1, 0x2, 0xe1, 0x0,
		0x78, 0x0, 0x0, 0x40, 0x6, 0x58, 0x76, 0x8c,
	}
	require.Equal(t, expectedBuffer, encoder.os.rawBuffer)
	require.Equal(t, 6, encoder.os.pos)

	expectedBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x20, 0x1, 0x50, 0x8,
		0x4, 0x0, 0x0, 0x0, 0x39, 0xbd, 0xf3, 0xb0, 0x1, 0x0, 0xa0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x2, 0x78, 0x65, 0x81, 0xd0, 0x4, 0x1, 0x2, 0x1, 0x2, 0xe1, 0x0,
		0x78, 0x0, 0x0, 0x40, 0x6, 0x58, 0x76, 0x8e, 0x0, 0x0,
	}
	require.Equal(t, expectedBytes, getBytes(t, encoder.Stream()))
}

func TestEncodeWithTimeUnit(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	require.Nil(t, encoder.Stream())

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

	for _, input := range inputs {
		encoder.Encode(input.dp, input.tu, nil)
	}

	expectedBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x40, 0x20, 0x0, 0x0,
		0x1, 0xcd, 0xef, 0x9d, 0x80, 0x8, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x13,
		0xc3, 0x2c, 0xe, 0x80, 0x38, 0x40, 0x10, 0x8, 0x13, 0xff, 0xff, 0xff, 0xc8,
		0x96, 0x18, 0x34, 0x3, 0x2c, 0x3b, 0x80, 0x40, 0x40, 0x0, 0x0, 0x0, 0x69,
		0xd, 0x9d, 0xa0, 0x11, 0xc0, 0x20, 0x1f, 0xff, 0xff, 0xff, 0xdf, 0xf6, 0x66,
		0xa0, 0x4, 0x51, 0x0, 0x0,
	}
	require.Equal(t, expectedBytes, getBytes(t, encoder.Stream()))
}

func TestEncodeWithAnnotationAndTimeUnit(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	require.Nil(t, encoder.Stream())

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

	for _, input := range inputs {
		encoder.Encode(input.dp, input.tu, input.ant)
	}

	expectedBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x20, 0x1, 0x50, 0x8,
		0x4, 0x0, 0x0, 0x0, 0x39, 0xbd, 0xf3, 0xb0, 0x1, 0x0, 0xa0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x2, 0x78, 0x65, 0x81, 0x80, 0x20, 0x40, 0x20, 0x5a, 0x0, 0x80,
		0x40, 0x40, 0x0, 0x0, 0x7, 0x73, 0x59, 0x40, 0x0, 0x8, 0x2, 0x8, 0x6, 0x8,
		0xb, 0xc3, 0xe9, 0x96, 0x1d, 0xc0, 0x20, 0x10, 0x0, 0x0, 0x0, 0x3, 0xb9,
		0xac, 0xa0, 0x8, 0xe0, 0x0,
	}

	require.Equal(t, expectedBytes, getBytes(t, encoder.Stream()))
}
