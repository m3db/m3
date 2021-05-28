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

package m3tsz

import (
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

var (
	testStartTime         = xtime.ToUnixNano(time.Unix(1427162400, 0))
	testDeterministicSeed = int64(testStartTime)
)

func getTestEncoder(startTime xtime.UnixNano) *encoder {
	return NewEncoder(startTime, nil, false, nil).(*encoder)
}

func getTestOptEncoder(startTime xtime.UnixNano) *encoder {
	return NewEncoder(startTime, nil, true, nil).(*encoder)
}

func TestWriteDeltaOfDeltaTimeUnitUnchanged(t *testing.T) {
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
		stream := encoding.NewOStream(nil, false, nil)
		tsEncoder := NewTimestampEncoder(testStartTime, input.timeUnit, encoding.NewOptions())
		tsEncoder.writeDeltaOfDeltaTimeUnitUnchanged(stream, 0, input.delta, input.timeUnit)
		b, p := stream.RawBytes()
		require.Equal(t, input.expectedBytes, b)
		require.Equal(t, input.expectedPos, p)
	}
}

func TestWriteDeltaOfDeltaTimeUnitChanged(t *testing.T) {
	inputs := []struct {
		delta         time.Duration
		expectedBytes []byte
		expectedPos   int
	}{
		{0, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, 8},
		{32 * time.Millisecond, []byte{0x0, 0x0, 0x0, 0x0, 0x1, 0xe8, 0x48, 0x0}, 8},
		{-63 * time.Microsecond, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x9, 0xe8}, 8},
	}
	for _, input := range inputs {
		stream := encoding.NewOStream(nil, false, nil)
		tsEncoder := NewTimestampEncoder(testStartTime, xtime.Nanosecond, encoding.NewOptions())
		tsEncoder.writeDeltaOfDeltaTimeUnitChanged(stream, 0, input.delta)
		b, p := stream.RawBytes()
		require.Equal(t, input.expectedBytes, b)
		require.Equal(t, input.expectedPos, p)
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
		encoder.Reset(testStartTime, 0, nil)
		eit := FloatEncoderAndIterator{PrevXOR: input.previousXOR}
		eit.writeXOR(encoder.os, input.currentXOR)
		b, p := encoder.os.RawBytes()
		require.Equal(t, input.expectedBytes, b)
		require.Equal(t, input.expectedPos, p)
	}
}

func TestWriteAnnotation(t *testing.T) {
	inputs := []struct {
		annotation    ts.Annotation
		expectedBytes []byte
		expectedPos   int
	}{
		{
			annotation:  nil,
			expectedPos: 0,
		},
		{
			annotation:    []byte{0x1, 0x2},
			expectedBytes: []byte{0x80, 0x20, 0x40, 0x20, 0x40},
			expectedPos:   3,
		},

		{
			annotation:    []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			expectedBytes: []byte{0x80, 0x21, 0xdf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xe0},
			expectedPos:   3,
		},
	}
	for _, input := range inputs {
		stream := encoding.NewOStream(nil, false, nil)
		tsEncoder := NewTimestampEncoder(0, xtime.Nanosecond, encoding.NewOptions())
		tsEncoder.writeAnnotation(stream, input.annotation)
		b, p := stream.RawBytes()
		require.Equal(t, input.expectedBytes, b)
		require.Equal(t, input.expectedPos, p)
	}
}

func getBytes(t *testing.T, e encoding.Encoder) []byte {
	ctx := context.NewBackground()
	defer ctx.Close()

	r, ok := e.Stream(ctx)
	if !ok {
		return nil
	}

	bytes, err := xio.ToBytes(r)
	assert.Equal(t, io.EOF, err)

	return bytes
}

func TestWriteTimeUnit(t *testing.T) {
	inputs := []struct {
		timeUnit       xtime.Unit
		expectedResult bool
		expectedBytes  []byte
		expectedPos    int
	}{
		{
			timeUnit:       xtime.None,
			expectedResult: false,
			expectedPos:    0,
		},
		{
			timeUnit:       xtime.Second,
			expectedResult: true,
			expectedBytes:  []byte{0x80, 0x40, 0x20},
			expectedPos:    3,
		},
		{
			timeUnit:       xtime.Unit(255),
			expectedResult: false,
			expectedPos:    0,
		},
	}
	for _, input := range inputs {
		stream := encoding.NewOStream(nil, false, nil)
		tsEncoder := NewTimestampEncoder(0, xtime.Nanosecond, encoding.NewOptions())
		tsEncoder.TimeUnit = xtime.None
		assert.Equal(t, input.expectedResult, tsEncoder.maybeWriteTimeUnitChange(stream, input.timeUnit))
		b, p := stream.RawBytes()
		assert.Equal(t, input.expectedBytes, b)
		assert.Equal(t, input.expectedPos, p)
	}
}

func TestEncodeNoAnnotation(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	encoder := getTestEncoder(testStartTime)
	_, ok := encoder.Stream(ctx)
	require.False(t, ok)

	startTime := xtime.ToUnixNano(time.Unix(1427162462, 0))
	inputs := []ts.Datapoint{
		{TimestampNanos: startTime, Value: 12},
		{TimestampNanos: startTime.Add(time.Second * 60), Value: 12},
		{TimestampNanos: startTime.Add(time.Second * 120), Value: 24},
		{TimestampNanos: startTime.Add(-time.Second * 76), Value: 24},
		{TimestampNanos: startTime.Add(-time.Second * 16), Value: 24},
		{TimestampNanos: startTime.Add(time.Second * 2092), Value: 15},
		{TimestampNanos: startTime.Add(time.Second * 4200), Value: 12},
	}
	for _, input := range inputs {
		encoder.Encode(input, xtime.Second, nil)
	}

	expectedBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x9f, 0x20, 0x14, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x5f, 0x8c, 0xb0, 0x3a, 0x0, 0xe1, 0x0, 0x78, 0x0, 0x0,
		0x40, 0x6, 0x58, 0x76, 0x8e, 0x0, 0x0,
	}
	require.Equal(t, expectedBytes, getBytes(t, encoder))

	expectedBuffer := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x9f, 0x20, 0x14, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x5f, 0x8c, 0xb0, 0x3a, 0x0, 0xe1, 0x0, 0x78, 0x0, 0x0,
		0x40, 0x6, 0x58, 0x76, 0x8c,
	}

	b, p := encoder.os.RawBytes()
	require.Equal(t, expectedBuffer, b)
	require.Equal(t, 6, p)
}

func TestEncodeWithAnnotation(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	encoder := getTestEncoder(testStartTime)
	_, ok := encoder.Stream(ctx)
	require.False(t, ok)

	startTime := xtime.ToUnixNano(time.Unix(1427162462, 0))
	inputs := []struct {
		dp  ts.Datapoint
		ant ts.Annotation
	}{
		{ts.Datapoint{TimestampNanos: startTime, Value: 12}, []byte{0xa}},
		{ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 60), Value: 12}, []byte{0xa}},
		{ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 120), Value: 24}, nil},
		{ts.Datapoint{TimestampNanos: startTime.Add(-time.Second * 76), Value: 24}, nil},
		{ts.Datapoint{TimestampNanos: startTime.Add(-time.Second * 16), Value: 24}, []byte{0x1, 0x2}},
		{ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 2092), Value: 15}, nil},
		{ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 4200), Value: 12}, nil},
	}

	for _, input := range inputs {
		encoder.Encode(input.dp, xtime.Second, input.ant)
	}

	expectedBuffer := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x20, 0x1, 0x53, 0xe4,
		0x2, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x7, 0x40, 0x10, 0x4,
		0x8, 0x4, 0xb, 0x84, 0x1, 0xe0, 0x0, 0x1, 0x0, 0x19, 0x61, 0xda, 0x30,
	}

	b, p := encoder.os.RawBytes()
	require.Equal(t, expectedBuffer, b)
	require.Equal(t, 4, p)

	expectedBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x20, 0x1, 0x53, 0xe4,
		0x2, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x7, 0x40, 0x10, 0x4,
		0x8, 0x4, 0xb, 0x84, 0x1, 0xe0, 0x0, 0x1, 0x0, 0x19, 0x61, 0xda, 0x38, 0x0,
	}
	require.Equal(t, expectedBytes, getBytes(t, encoder))
}

func TestEncodeWithTimeUnit(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	encoder := getTestEncoder(testStartTime)
	_, ok := encoder.Stream(ctx)
	require.False(t, ok)

	startTime := xtime.ToUnixNano(time.Unix(1427162462, 0))
	inputs := []struct {
		dp ts.Datapoint
		tu xtime.Unit
	}{
		{ts.Datapoint{TimestampNanos: startTime, Value: 12}, xtime.Second},
		{ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 60), Value: 12}, xtime.Second},
		{ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 120), Value: 24}, xtime.Second},
		{ts.Datapoint{TimestampNanos: startTime.Add(-time.Second * 76), Value: 24}, xtime.Second},
		{ts.Datapoint{TimestampNanos: startTime.Add(-time.Second * 16), Value: 24}, xtime.Second},
		{ts.Datapoint{TimestampNanos: startTime.Add(-time.Nanosecond * 15500000000), Value: 15}, xtime.Nanosecond},
		{ts.Datapoint{TimestampNanos: startTime.Add(-time.Millisecond * 1400), Value: 12}, xtime.Millisecond},
		{ts.Datapoint{TimestampNanos: startTime.Add(-time.Second * 10), Value: 12}, xtime.Second},
		{ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 10), Value: 12}, xtime.Second},
	}

	for _, input := range inputs {
		encoder.Encode(input.dp, input.tu, nil)
	}

	expectedBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x9f, 0x20, 0x14, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x5f, 0x8c, 0xb0, 0x3a, 0x0, 0xe1, 0x0, 0x40, 0x20,
		0x4f, 0xff, 0xff, 0xff, 0x22, 0x58, 0x60, 0xd0, 0xc, 0xb0, 0xee, 0x1, 0x1,
		0x0, 0x0, 0x0, 0x1, 0xa4, 0x36, 0x76, 0x80, 0x47, 0x0, 0x80, 0x7f, 0xff,
		0xff, 0xff, 0x7f, 0xd9, 0x9a, 0x80, 0x11, 0x44, 0x0,
	}
	require.Equal(t, expectedBytes, getBytes(t, encoder))
}

func TestEncodeWithAnnotationAndTimeUnit(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	encoder := getTestEncoder(testStartTime)
	_, ok := encoder.Stream(ctx)
	require.False(t, ok)

	startTime := xtime.ToUnixNano(time.Unix(1427162462, 0))
	inputs := []struct {
		dp  ts.Datapoint
		ant ts.Annotation
		tu  xtime.Unit
	}{
		{
			dp:  ts.Datapoint{TimestampNanos: startTime, Value: 12},
			ant: []byte{0xa},
			tu:  xtime.Second,
		},
		{
			dp:  ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 60), Value: 12},
			ant: nil,
			tu:  xtime.Second,
		},
		{
			dp:  ts.Datapoint{TimestampNanos: startTime.Add(time.Second * 120), Value: 24},
			ant: nil,
			tu:  xtime.Second,
		},
		{
			dp:  ts.Datapoint{TimestampNanos: startTime.Add(-time.Second * 76), Value: 24},
			ant: []byte{0x1, 0x2},
			tu:  xtime.Second,
		},
		{
			dp:  ts.Datapoint{TimestampNanos: startTime.Add(-time.Second * 16), Value: 24},
			ant: nil,
			tu:  xtime.Millisecond,
		},
		{
			dp:  ts.Datapoint{TimestampNanos: startTime.Add(-time.Millisecond * 15500), Value: 15},
			ant: []byte{0x3, 0x4, 0x5},
			tu:  xtime.Millisecond,
		},
		{
			dp:  ts.Datapoint{TimestampNanos: startTime.Add(-time.Millisecond * 14000), Value: 12},
			ant: nil,
			tu:  xtime.Second,
		},
	}

	for _, input := range inputs {
		encoder.Encode(input.dp, input.tu, input.ant)
	}

	expectedBytes := []byte{
		0x13, 0xce, 0x4c, 0xa4, 0x30, 0xcb, 0x40, 0x0, 0x80, 0x20, 0x1, 0x53, 0xe4,
		0x2, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x6, 0x0, 0x81, 0x0,
		0x81, 0x68, 0x2, 0x1, 0x1, 0x0, 0x0, 0x0, 0x1d, 0xcd, 0x65, 0x0, 0x0, 0x20,
		0x8, 0x20, 0x18, 0x20, 0x2f, 0xf, 0xa6, 0x58, 0x77, 0x0, 0x80, 0x40, 0x0,
		0x0, 0x0, 0xe, 0xe6, 0xb2, 0x80, 0x23, 0x80, 0x0,
	}

	require.Equal(t, expectedBytes, getBytes(t, encoder))
}

func TestInitTimeUnit(t *testing.T) {
	inputs := []struct {
		start    time.Time
		tu       xtime.Unit
		expected xtime.Unit
	}{
		{time.Unix(1, 0), xtime.Second, xtime.Second},
		{time.Unix(1, 1000), xtime.Second, xtime.None},
		{time.Unix(1, 1000), xtime.Microsecond, xtime.Microsecond},
		{time.Unix(1, 1000), xtime.None, xtime.None},
		{time.Unix(1, 1000), xtime.Unit(9), xtime.None},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, initialTimeUnit(xtime.ToUnixNano(input.start), input.tu))
	}
}

func TestEncoderResets(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	enc := getTestOptEncoder(testStartTime)
	defer enc.Close()

	require.Equal(t, 0, enc.os.Len())
	_, ok := enc.Stream(ctx)
	require.False(t, ok)

	err := enc.Encode(ts.Datapoint{TimestampNanos: testStartTime, Value: 12}, xtime.Second, nil)
	require.NoError(t, err)
	require.True(t, enc.os.Len() > 0)

	now := xtime.Now()
	enc.Reset(now, 0, nil)
	require.Equal(t, 0, enc.os.Len())
	_, ok = enc.Stream(ctx)
	require.False(t, ok)
	b, _ := enc.os.RawBytes()
	require.Equal(t, []byte{}, b)

	err = enc.Encode(ts.Datapoint{TimestampNanos: now, Value: 13}, xtime.Second, nil)
	require.NoError(t, err)
	require.True(t, enc.os.Len() > 0)

	enc.DiscardReset(now, 0, nil)
	require.Equal(t, 0, enc.os.Len())
	_, ok = enc.Stream(ctx)
	require.False(t, ok)
	b, _ = enc.os.RawBytes()
	require.Equal(t, []byte{}, b)
}

func TestEncoderNumEncoded(t *testing.T) {
	testMultiplePasses(t, multiplePassesTest{
		postEncodeAll: func(enc *encoder, numDatapointsEncoded int) {
			assert.Equal(t, numDatapointsEncoded, enc.NumEncoded())
		},
	})
}

func TestEncoderLastEncoded(t *testing.T) {
	testMultiplePasses(t, multiplePassesTest{
		postEncodeDatapoint: func(enc *encoder, datapoint ts.Datapoint) {
			last, err := enc.LastEncoded()
			require.NoError(t, err)
			assert.Equal(t, datapoint.TimestampNanos, last.TimestampNanos)
			assert.Equal(t, datapoint.Value, datapoint.Value)
		},
	})
}

func TestEncoderLenReturnsFinalStreamLength(t *testing.T) {
	testMultiplePasses(t, multiplePassesTest{
		postEncodeAll: func(enc *encoder, numDatapointsEncoded int) {
			ctx := context.NewBackground()
			defer ctx.BlockingClose()

			encLen := enc.Len()
			stream, ok := enc.Stream(ctx)

			var streamLen int
			if ok {
				segment, err := stream.Segment()
				require.NoError(t, err)
				streamLen = segment.Len()
			}
			require.Equal(t, streamLen, encLen)
		},
	})
}

type testFinalizer struct {
	t                *testing.T
	numActiveStreams *atomic.Int32
}

func (f *testFinalizer) FinalizeBytes(bytes checked.Bytes) {
	require.Equal(f.t, int32(0), f.numActiveStreams.Load(),
		"expect 0 active streams when finalizing the byte slice")
}

func TestEncoderCloseWaitForStream(t *testing.T) {
	numActiveStreams := atomic.NewInt32(0)

	finalizer := &testFinalizer{t: t, numActiveStreams: numActiveStreams}
	bytesOptions := checked.NewBytesOptions().SetFinalizer(finalizer)
	cb := checked.NewBytes(make([]byte, 16), bytesOptions)
	enc := NewEncoder(testStartTime, cb, false, nil).(*encoder)
	defer enc.Close()

	numStreams := 8
	for i := 0; i <= numStreams; i++ {
		numActiveStreams.Inc()
		ctx := context.NewBackground()
		_, ok := enc.Stream(ctx)
		require.True(t, ok)
		go func(ctx context.Context, idx int) {
			time.Sleep(time.Duration(idx*rand.Intn(100)) * time.Millisecond)
			numActiveStreams.Dec()
			ctx.Close()
		}(ctx, i)
	}
}

type multiplePassesTest struct {
	preEncodeAll        func(enc *encoder, numDatapointsToEncode int)
	preEncodeDatapoint  func(enc *encoder, datapoint ts.Datapoint)
	postEncodeDatapoint func(enc *encoder, datapoint ts.Datapoint)
	postEncodeAll       func(enc *encoder, numDatapointsEncoded int)
}

func testMultiplePasses(t *testing.T, test multiplePassesTest) {
	src := rand.NewSource(testDeterministicSeed)
	rng := rand.New(src)
	maxValues := 512

	for n := 0; n < 1024; n++ {
		encoder := getTestEncoder(testStartTime)

		numValues := int(rng.Int63()) % maxValues
		// Check boundary cases
		switch n {
		case 0:
			numValues = 0
		case 1:
			numValues = 1
		}

		now := testStartTime
		if test.preEncodeAll != nil {
			test.preEncodeAll(encoder, numValues)
		}

		for i := 0; i < numValues; i++ {
			now = now.Add(time.Duration(rng.Int63()) % time.Minute)
			value := ts.Datapoint{
				TimestampNanos: now,
				Value:          rng.NormFloat64(),
			}

			if test.preEncodeDatapoint != nil {
				test.preEncodeDatapoint(encoder, value)
			}

			err := encoder.Encode(value, xtime.Nanosecond, nil)
			require.NoError(t, err)

			if test.postEncodeDatapoint != nil {
				test.postEncodeDatapoint(encoder, value)
			}
		}

		if test.postEncodeAll != nil {
			test.postEncodeAll(encoder, numValues)
		}
	}
}

func TestEncoderFailOnDeltaOfDeltaOverflow(t *testing.T) {
	tests := []struct {
		name           string
		delta          time.Duration
		units          xtime.Unit
		positiveErrMsg string
		negativeErrMsg string
	}{
		{
			name:  "seconds, short gap",
			delta: time.Hour,
			units: xtime.Second,
		},
		{
			name:  "seconds, huge gap",
			delta: 25 * 24 * time.Hour,
			units: xtime.Second,
		},
		{
			name:           "seconds, too big gap",
			delta:          1000 * 25 * 24 * time.Hour, // more than 2^31 s
			units:          xtime.Second,
			positiveErrMsg: "deltaOfDelta value 2160000000 s overflows 32 bits",
			negativeErrMsg: "deltaOfDelta value -2159999999 s overflows 32 bits",
		},
		{
			name:  "milliseconds, short gap",
			delta: time.Hour,
			units: xtime.Millisecond,
		},
		{
			name:  "milliseconds, almost too big gap",
			delta: 24 * 24 * time.Hour, // slightly less than 2^31 ms
			units: xtime.Millisecond,
		},
		{
			name:           "milliseconds, too big gap",
			delta:          25 * 24 * time.Hour, // more than 2^31 ms
			units:          xtime.Millisecond,
			positiveErrMsg: "deltaOfDelta value 2160000000 ms overflows 32 bits",
			negativeErrMsg: "deltaOfDelta value -2159999999 ms overflows 32 bits",
		},
		{
			name:  "microseconds, short gap",
			delta: time.Hour,
			units: xtime.Microsecond,
		},
		{
			name:  "microseconds, huge gap",
			delta: 25 * 24 * time.Hour,
			units: xtime.Microsecond,
		},
		{
			name:  "nanoseconds, short gap",
			delta: time.Hour,
			units: xtime.Nanosecond,
		},
		{
			name:  "nanoseconds, huge gap",
			delta: 25 * 24 * time.Hour,
			units: xtime.Nanosecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testPositiveDeltaOfDelta(t, tt.delta, tt.units, tt.positiveErrMsg)
			testNegativeDeltaOfDelta(t, tt.delta, tt.units, tt.negativeErrMsg)
		})
	}
}

func testPositiveDeltaOfDelta(t *testing.T, delta time.Duration, units xtime.Unit, expectedErrMsg string) {
	t.Helper()

	ctx := context.NewBackground()
	defer ctx.Close()

	enc := getTestEncoder(testStartTime)

	dp1 := dp(testStartTime, 1)
	dp2 := dp(testStartTime.Add(delta), 2)

	err := enc.Encode(dp1, units, nil)
	require.NoError(t, err)

	err = enc.Encode(dp2, units, nil)
	if expectedErrMsg != "" {
		require.EqualError(t, err, expectedErrMsg)
		return
	}
	require.NoError(t, err)

	dec := NewDecoder(enc.intOptimized, enc.opts)
	stream, ok := enc.Stream(ctx)
	require.True(t, ok)

	it := dec.Decode(stream)
	defer it.Close()

	decodeAndCheck(t, it, dp1, units)
	decodeAndCheck(t, it, dp2, units)

	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

func testNegativeDeltaOfDelta(
	t *testing.T, delta time.Duration, units xtime.Unit, expectedErrMsg string,
) {
	t.Helper()

	ctx := context.NewBackground()
	defer ctx.Close()

	oneUnit, err := units.Value()
	require.NoError(t, err)

	enc := getTestEncoder(testStartTime)

	dps := []ts.Datapoint{
		dp(testStartTime, 1),
		dp(testStartTime.Add(delta/2), 2),
		dp(testStartTime.Add(delta/2+delta), 3),
		dp(testStartTime.Add(delta/2+delta+oneUnit), 4), // DoD = oneUnit - delta
	}

	for i, dp := range dps {
		err = enc.Encode(dp, units, nil)
		if i == 3 && expectedErrMsg != "" {
			require.EqualError(t, err, expectedErrMsg)
			return
		}
		require.NoError(t, err)
	}

	dec := NewDecoder(enc.intOptimized, enc.opts)
	stream, ok := enc.Stream(ctx)
	require.True(t, ok)

	it := dec.Decode(stream)
	defer it.Close()

	for _, dp := range dps {
		decodeAndCheck(t, it, dp, units)
	}

	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

func dp(timestamp xtime.UnixNano, value float64) ts.Datapoint {
	return ts.Datapoint{
		TimestampNanos: timestamp,
		Value:          value,
	}
}

func decodeAndCheck(
	t *testing.T, it encoding.ReaderIterator, dp ts.Datapoint, units xtime.Unit,
) {
	t.Helper()

	require.True(t, it.Next())
	decodedDP, decodedUnits, _ := it.Current()
	assert.Equal(t, dp, decodedDP)
	assert.Equal(t, units, decodedUnits)
}
