package tsz

import (
	"bytes"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	"github.com/stretchr/testify/require"
)

func getTestIterator(rawBytes []byte, timeUnit time.Duration) *iterator {
	return newIterator(bytes.NewReader(rawBytes), timeUnit).(*iterator)
}

func TestReadNonFirstTimestamp(t *testing.T) {
	inputs := []struct {
		previousTime      int64
		previousTimeDelta int64
		rawBytes          []byte
		expectedTimeDelta int64
		expectedTime      int64
	}{
		{10, 62, []byte{0x0}, 62, 72},
		{20, 65, []byte{0x1, 0x1}, 1, 21},
		{30, 65, []byte{0x81, 0x0}, 97, 127},
		{40, 65, []byte{0x3, 0x8}, -191, -151},
		{50, 65, []byte{0xfb, 0x7}, 320, 370},
		{60, 65, []byte{0x7, 0x80}, -1983, -1923},
		{70, 65, []byte{0xf7, 0x7f}, 2112, 2182},
		{80, 65, []byte{0xf, 0x0, 0x1, 0x0, 0x0}, 4161, 4241},
		{90, 65, []byte{0xf, 0x0, 0xff, 0xff, 0xf}, -4031, -3941},
	}
	for _, input := range inputs {
		it := getTestIterator(input.rawBytes, time.Second)
		it.nt = input.previousTime
		it.dt = input.previousTimeDelta
		it.readNonFirstTimestamp()
		require.Equal(t, input.expectedTimeDelta, it.dt)
		require.Equal(t, input.expectedTime, it.nt)
		require.NoError(t, it.Err())
	}

	it := getTestIterator([]byte{0x1}, time.Second)
	it.readNonFirstTimestamp()
	require.Error(t, it.Err())
	it.readNonFirstTimestamp()
	require.Error(t, it.Err())
}

func TestReadNonFirstValue(t *testing.T) {
	inputs := []struct {
		previousValue    uint64
		previousValueXOR uint64
		rawBytes         []byte
		expectedValueXOR uint64
		expectedValue    uint64
	}{
		{0x1234, 0x4028000000000000, []byte{0x0}, 0x0, 0x1234},
		{0xaaaaaa, 0x4028000000000000, []byte{0x91, 0x0}, 0x0120000000000000, 0x0120000000aaaaaa},
		{0xdeadbeef, 0x0120000000000000, []byte{0x7, 0x4b, 0x1, 0x2}, 0x4028000000000000, 0x40280000deadbeef},
	}
	for _, input := range inputs {
		it := getTestIterator(input.rawBytes, time.Second)
		it.vb = input.previousValue
		it.xor = input.previousValueXOR
		it.readNonFirstValue()
		require.Equal(t, input.expectedValueXOR, it.xor)
		require.Equal(t, input.expectedValue, it.vb)
		require.NoError(t, it.Err())
	}

	it := getTestIterator([]byte{0x3}, time.Second)
	it.readNonFirstValue()
	require.Error(t, it.Err())
}

func TestNext(t *testing.T) {
	rawBytes := []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0xf9, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x50, 0x80, 0xf2, 0xf3, 0x2, 0x7c, 0x0, 0x0, 0x0, 0x0,
	}
	startTime := time.Unix(1427162462, 0)
	inputs := []encoding.Datapoint{
		{startTime, 12},
		{startTime.Add(time.Second * 60), 12},
		{startTime.Add(time.Second * 120), 24},
	}
	it := getTestIterator(rawBytes, time.Second)
	for i := 0; i < 3; i++ {
		require.True(t, it.Next())
		require.Equal(t, inputs[i].Timestamp, it.Value().Timestamp)
		require.Equal(t, inputs[i].Value, it.Value().Value)
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

	it = getTestIterator([]byte{0x3}, time.Second)
	it.readNonFirstValue()
	require.False(t, it.Next())
	require.False(t, it.isDone())
	require.True(t, it.hasError())
}
