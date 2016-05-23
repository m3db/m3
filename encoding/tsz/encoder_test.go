package tsz

import (
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	"github.com/stretchr/testify/require"
)

var (
	testStartTime = time.Unix(1427162400, 0)
)

func getTestEncoder(startTime time.Time, timeUnit time.Duration) *encoder {
	return NewEncoder(startTime, timeUnit).(*encoder)
}

func TestWriteDeltaOfDelta(t *testing.T) {
	encoder := getTestEncoder(testStartTime, time.Second)
	inputs := []struct {
		deltaOfDelta  int64
		expectedBytes []byte
		expectedPos   int
	}{
		{0, []byte{0x0}, 1},
		{32, []byte{0x81, 0x0}, 1},
		{-63, []byte{0x5, 0x1}, 1},
		{-128, []byte{0x3, 0xc}, 4},
		{255, []byte{0xfb, 0x7}, 4},
		{-2048, []byte{0x7, 0x80}, 8},
		{2047, []byte{0xf7, 0x7f}, 8},
		{4096, []byte{0xf, 0x0, 0x1, 0x0, 0x0}, 4},
		{-4096, []byte{0xf, 0x0, 0xff, 0xff, 0xf}, 4},
	}
	for _, input := range inputs {
		encoder.Reset(testStartTime)
		encoder.writeDeltaOfDelta(0, input.deltaOfDelta)
		require.Equal(t, input.expectedBytes, encoder.os.rawBuffer)
		require.Equal(t, input.expectedPos, encoder.os.pos)
	}
}

func TestWriteValue(t *testing.T) {
	encoder := getTestEncoder(testStartTime, time.Second)
	inputs := []struct {
		previousXOR   uint64
		currentXOR    uint64
		expectedBytes []byte
		expectedPos   int
	}{
		{0x4028000000000000, 0, []byte{0x0}, 1},
		{0x4028000000000000, 0x0120000000000000, []byte{0x91, 0x0}, 6},
		{0x0120000000000000, 0x4028000000000000, []byte{0x7, 0x4b, 0x1, 0x2}, 2},
	}
	for _, input := range inputs {
		encoder.Reset(testStartTime)
		encoder.writeXOR(input.previousXOR, input.currentXOR)
		require.Equal(t, input.expectedBytes, encoder.os.rawBuffer)
		require.Equal(t, input.expectedPos, encoder.os.pos)
	}
}

func TestWriteAnnotation(t *testing.T) {
	encoder := getTestEncoder(testStartTime, time.Second)
	inputs := []struct {
		annotation    encoding.Annotation
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
			[]byte{0x1f, 0x0, 0x0, 0x0, 0x20, 0x10, 0x20, 0x0},
			4,
		},
		{
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			[]byte{0x1f, 0x0, 0x0, 0x0, 0xe0, 0xf0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf},
			4,
		},
	}
	for _, input := range inputs {
		encoder.Reset(testStartTime)
		encoder.writeAnnotation(input.annotation)
		require.Equal(t, input.expectedBytes, encoder.os.rawBuffer)
		require.Equal(t, input.expectedPos, encoder.os.pos)
	}
}

func TestEncodeNoAnnotation(t *testing.T) {
	encoder := getTestEncoder(testStartTime, time.Second)
	require.Nil(t, encoder.Bytes())

	startTime := time.Unix(1427162462, 0)
	inputs := []encoding.Datapoint{
		{startTime, 12},
		{startTime.Add(time.Second * 60), 12},
		{startTime.Add(time.Second * 120), 24},
		{startTime.Add(-time.Second * 76), 24},
		{startTime.Add(-time.Second * 16), 24},
		{startTime.Add(time.Second * 2092), 15},
		{startTime.Add(time.Second * 4200), 12},
	}
	for _, input := range inputs {
		encoder.Encode(input, nil)
	}

	expectedBytes := []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x50, 0x80, 0xf2, 0xf3, 0x2, 0x1c, 0x40, 0x7, 0x10,
		0x1e, 0x0, 0x1, 0x0, 0xe0, 0x65, 0x58, 0xcd, 0x3, 0x0, 0x0, 0x0, 0x0,
	}
	require.Equal(t, expectedBytes, encoder.Bytes())

	expectedBuffer := []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x50, 0x80, 0xf2, 0xf3, 0x2, 0x1c, 0x40, 0x7, 0x10,
		0x1e, 0x0, 0x1, 0x0, 0xe0, 0x65, 0x58, 0xcd,
	}
	require.Equal(t, expectedBuffer, encoder.os.rawBuffer)
	require.Equal(t, 6, encoder.os.pos)
}

func TestEncodeWithAnnotation(t *testing.T) {
	encoder := getTestEncoder(testStartTime, time.Second)
	require.Nil(t, encoder.Bytes())

	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp  encoding.Datapoint
		ant encoding.Annotation
	}{
		{encoding.Datapoint{startTime, 12}, []byte{0xa}},
		{encoding.Datapoint{startTime.Add(time.Second * 60), 12}, nil},
		{encoding.Datapoint{startTime.Add(time.Second * 120), 24}, nil},
		{encoding.Datapoint{startTime.Add(-time.Second * 76), 24}, nil},
		{encoding.Datapoint{startTime.Add(-time.Second * 16), 24}, []byte{0x1, 0x2}},
		{encoding.Datapoint{startTime.Add(time.Second * 2092), 15}, nil},
		{encoding.Datapoint{startTime.Add(time.Second * 4200), 12}, nil},
	}

	for _, input := range inputs {
		encoder.Encode(input.dp, input.ant)
	}

	expectedBuffer := []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0x1f, 0x0, 0x0, 0x0, 0x0,
		0xa0, 0x90, 0xf, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x28, 0x3f, 0x2f,
		0xc0, 0x1, 0xf4, 0x1, 0x0, 0x0, 0x0, 0x2, 0x1, 0x2, 0x7, 0x10, 0x1e,
		0x0, 0x1, 0x0, 0xe0, 0x65, 0x58, 0xd,
	}
	require.Equal(t, expectedBuffer, encoder.os.rawBuffer)
	require.Equal(t, 6, encoder.os.pos)

	expectedBytes := []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0x1f, 0x0, 0x0, 0x0, 0x0,
		0xa0, 0x90, 0xf, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x28, 0x3f, 0x2f,
		0xc0, 0x1, 0xf4, 0x1, 0x0, 0x0, 0x0, 0x2, 0x1, 0x2, 0x7, 0x10, 0x1e,
		0x0, 0x1, 0x0, 0xe0, 0x65, 0x58, 0xcd, 0x3, 0x0, 0x0, 0x0, 0x0,
	}
	require.Equal(t, expectedBytes, encoder.Bytes())
}
