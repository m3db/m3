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

	"github.com/m3db/m3db"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testStartTime = time.Unix(1427162400, 0)
)

func getTestEncoder(startTime time.Time) *encoder {
	return NewEncoder(startTime, nil, nil).(*encoder)
}

func TestWriteDeltaOfDelta(t *testing.T) {
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
		annotation    memtsdb.Annotation
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
	inputs := []memtsdb.Datapoint{
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
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x40, 0x33, 0xe4,
		0x2, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x7, 0x40, 0x1c,
		0x20, 0xf, 0x0, 0x0, 0x8, 0x0, 0xcb, 0xe, 0xd1, 0xc0, 0x0,
	}
	require.Equal(t, expectedBytes, getBytes(t, encoder.Stream()))

	expectedBuffer := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x40, 0x33, 0xe4,
		0x2, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x7, 0x40, 0x1c,
		0x20, 0xf, 0x0, 0x0, 0x8, 0x0, 0xcb, 0xe, 0xd1, 0x80,
	}
	require.Equal(t, expectedBuffer, encoder.os.rawBuffer)
	require.Equal(t, 1, encoder.os.pos)
}

func TestEncodeWithAnnotation(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	require.Nil(t, encoder.Stream())

	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp  memtsdb.Datapoint
		ant memtsdb.Annotation
	}{
		{memtsdb.Datapoint{startTime, 12}, []byte{0xa}},
		{memtsdb.Datapoint{startTime.Add(time.Second * 60), 12}, []byte{0xa}},
		{memtsdb.Datapoint{startTime.Add(time.Second * 120), 24}, nil},
		{memtsdb.Datapoint{startTime.Add(-time.Second * 76), 24}, nil},
		{memtsdb.Datapoint{startTime.Add(-time.Second * 16), 24}, []byte{0x1, 0x2}},
		{memtsdb.Datapoint{startTime.Add(time.Second * 2092), 15}, nil},
		{memtsdb.Datapoint{startTime.Add(time.Second * 4200), 12}, nil},
	}

	for _, input := range inputs {
		encoder.Encode(input.dp, xtime.Second, input.ant)
	}

	expectedBuffer := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x20, 0x1, 0x50, 0x8,
		0x6, 0x7c, 0x80, 0x50, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x7e, 0x32, 0xc0,
		0xe8, 0x2, 0x0, 0x81, 0x0, 0x81, 0x70, 0x80, 0x3c, 0x0, 0x0, 0x20, 0x3,
		0x2c, 0x3b, 0x46,
	}
	require.Equal(t, expectedBuffer, encoder.os.rawBuffer)
	require.Equal(t, 7, encoder.os.pos)

	expectedBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x20, 0x1, 0x50, 0x8,
		0x6, 0x7c, 0x80, 0x50, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x7e, 0x32, 0xc0,
		0xe8, 0x2, 0x0, 0x81, 0x0, 0x81, 0x70, 0x80, 0x3c, 0x0, 0x0, 0x20, 0x3,
		0x2c, 0x3b, 0x47, 0x0, 0x0,
	}
	require.Equal(t, expectedBytes, getBytes(t, encoder.Stream()))
}

func TestEncodeWithTimeUnit(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	require.Nil(t, encoder.Stream())

	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp memtsdb.Datapoint
		tu xtime.Unit
	}{
		{memtsdb.Datapoint{startTime, 12}, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(time.Second * 60), 12}, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(time.Second * 120), 24}, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(-time.Second * 76), 24}, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(-time.Second * 16), 24}, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(-time.Nanosecond * 15500000000), 15}, xtime.Nanosecond},
		{memtsdb.Datapoint{startTime.Add(-time.Nanosecond * 14000000000), 12}, xtime.Second},
	}

	for _, input := range inputs {
		encoder.Encode(input.dp, input.tu, nil)
	}

	expectedBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x40, 0x33, 0xe4, 0x2,
		0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x7, 0x40, 0x1c, 0x20, 0x8,
		0x4, 0x9, 0xff, 0xff, 0xff, 0xfe, 0x44, 0xb0, 0xc1, 0xa0, 0x19, 0x61, 0xdc,
		0x2, 0x1, 0x80, 0xc7, 0x0, 0x0,
	}

	require.Equal(t, expectedBytes, getBytes(t, encoder.Stream()))
}

func TestEncodeWithAnnotationAndTimeUnit(t *testing.T) {
	encoder := getTestEncoder(testStartTime)
	require.Nil(t, encoder.Stream())

	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp  memtsdb.Datapoint
		ant memtsdb.Annotation
		tu  xtime.Unit
	}{
		{memtsdb.Datapoint{startTime, 12}, []byte{0xa}, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(time.Second * 60), 12}, nil, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(time.Second * 120), 24}, nil, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(-time.Second * 76), 24}, []byte{0x1, 0x2}, xtime.Second},
		{memtsdb.Datapoint{startTime.Add(-time.Second * 16), 24}, nil, xtime.Millisecond},
		{memtsdb.Datapoint{startTime.Add(-time.Millisecond * 15500), 15}, []byte{0x3, 0x4, 0x5}, xtime.Millisecond},
		{memtsdb.Datapoint{startTime.Add(-time.Millisecond * 14000), 12}, nil, xtime.Second},
	}

	for _, input := range inputs {
		encoder.Encode(input.dp, input.tu, input.ant)
	}

	expectedBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x20, 0x1, 0x50, 0x8,
		0x6, 0x7c, 0x80, 0x50, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x7e, 0x32, 0xc0,
		0xc0, 0x10, 0x20, 0x10, 0x2d, 0x0, 0x40, 0x20, 0x2f, 0x0, 0x3, 0xe8, 0x0,
		0x40, 0x10, 0x40, 0x30, 0x40, 0x5f, 0xff, 0xff, 0x17, 0x94, 0xcb, 0xe,
		0xe0, 0x10, 0xc, 0x6, 0x38, 0x0,
	}

	require.Equal(t, expectedBytes, getBytes(t, encoder.Stream()))
}
