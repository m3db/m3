package tsz

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"github.com/stretchr/testify/require"
)

func getTestIterator(rawBytes []byte) *iterator {
	return NewIterator(bytes.NewReader(rawBytes), NewOptions()).(*iterator)
}

func TestReadNextTimestamp(t *testing.T) {
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
	}

	for _, input := range inputs {
		it := getTestIterator(input.rawBytes)
		it.tu = input.timeUnit
		it.dt = input.previousTimeDelta
		it.readNextTimestamp()
		require.Equal(t, input.expectedTimeDelta, it.dt)
		require.NoError(t, it.Err())
	}

	it := getTestIterator([]byte{0x1})
	it.readNextTimestamp()
	require.Error(t, it.Err())
	it.readNextTimestamp()
	require.Error(t, it.Err())
}

func TestReadNextValue(t *testing.T) {
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
		it := getTestIterator(input.rawBytes)
		it.vb = input.previousValue
		it.xor = input.previousValueXOR
		it.readNextValue()
		require.Equal(t, input.expectedValueXOR, it.xor)
		require.Equal(t, input.expectedValue, it.vb)
		require.NoError(t, it.Err())
	}

	it := getTestIterator([]byte{0xf0})
	it.readNextValue()
	require.Error(t, it.Err())
}

func TestReadAnnotation(t *testing.T) {
	inputs := []struct {
		rawBytes           []byte
		expectedAnnotation memtsdb.Annotation
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
		it := getTestIterator(input.rawBytes)
		it.readAnnotation()
		require.Equal(t, input.expectedAnnotation, it.ant)
	}
}

func TestReadTimeUnit(t *testing.T) {
	inputs := []struct {
		rawBytes         []byte
		expectedTimeUnit xtime.Unit
	}{
		{
			[]byte{0x1},
			xtime.Second,
		},
		{
			[]byte{0x0},
			xtime.None,
		},
	}
	for _, input := range inputs {
		it := getTestIterator(input.rawBytes)
		it.readTimeUnit()
		require.NoError(t, it.err)
		require.Equal(t, input.expectedTimeUnit, it.tu)
	}
}

func TestNextNoAnnotation(t *testing.T) {
	rawBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x40, 0x33, 0xe4,
		0x2, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x7, 0x40, 0x1c,
		0x20, 0xf, 0x0, 0x0, 0x8, 0x0, 0xcb, 0xe, 0xd1, 0xc0, 0x0,
	}
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
	it := getTestIterator(rawBytes)
	for i := 0; i < 7; i++ {
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

	it = getTestIterator([]byte{0xf0})
	it.readNextValue()
	require.False(t, it.Next())
	require.False(t, it.isDone())
	require.True(t, it.hasError())
}

func TestNextWithAnnotation(t *testing.T) {
	rawBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x20, 0x1, 0x50, 0x8,
		0x6, 0x7c, 0x80, 0x50, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x7e, 0x32, 0xc0,
		0xe8, 0x2, 0x0, 0x81, 0x0, 0x81, 0x70, 0x80, 0x3c, 0x0, 0x0, 0x20, 0x3,
		0x2c, 0x3b, 0x47, 0x0, 0x0,
	}
	startTime := time.Unix(1427162462, 0)
	inputs := []struct {
		dp  memtsdb.Datapoint
		ant memtsdb.Annotation
	}{
		{memtsdb.Datapoint{startTime, 12}, []byte{0xa}},
		{memtsdb.Datapoint{startTime.Add(time.Second * 60), 12}, nil},
		{memtsdb.Datapoint{startTime.Add(time.Second * 120), 24}, nil},
		{memtsdb.Datapoint{startTime.Add(-time.Second * 76), 24}, nil},
		{memtsdb.Datapoint{startTime.Add(-time.Second * 16), 24}, []byte{0x1, 0x2}},
		{memtsdb.Datapoint{startTime.Add(time.Second * 2092), 15}, nil},
		{memtsdb.Datapoint{startTime.Add(time.Second * 4200), 12}, nil},
	}
	it := getTestIterator(rawBytes)
	for i := 0; i < 7; i++ {
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

	it = getTestIterator(
		[]byte{0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x20, 0x1, 0x50, 0x8},
	)
	require.False(t, it.Next())
	require.False(t, it.isDone())
	require.True(t, it.hasError())
}

func TestNextWithTimeUnit(t *testing.T) {
	rawBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x40, 0x33, 0xe4, 0x2,
		0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xf1, 0x96, 0x7, 0x40, 0x1c, 0x20, 0x8,
		0x4, 0x9, 0xff, 0xff, 0xff, 0xfe, 0x44, 0xb0, 0xc1, 0xa0, 0x19, 0x61, 0xdc,
		0x2, 0x1, 0x80, 0xc7, 0x0, 0x0,
	}
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
	it := getTestIterator(rawBytes)
	for i := 0; i < 7; i++ {
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

func TestNextWithAnnotationAndTimeUnit(t *testing.T) {
	rawBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x20, 0x1, 0x50, 0x8,
		0x6, 0x7c, 0x80, 0x50, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x7e, 0x32, 0xc0,
		0xc0, 0x10, 0x20, 0x10, 0x2d, 0x0, 0x40, 0x20, 0x2f, 0x0, 0x3, 0xe8, 0x0,
		0x40, 0x10, 0x40, 0x30, 0x40, 0x5f, 0xff, 0xff, 0x17, 0x94, 0xcb, 0xe,
		0xe0, 0x10, 0xc, 0x6, 0x38, 0x0,
	}
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
	it := getTestIterator(rawBytes)
	for i := 0; i < 7; i++ {
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

func TestNextWithUnexpectedTimeUnit(t *testing.T) {
	rawBytes := []byte{
		0x0, 0x0, 0x0, 0x0, 0x55, 0x10, 0xc5, 0x20, 0x80, 0x41, 0x20, 0x0, 0x0,
	}
	it := getTestIterator(rawBytes)
	require.False(t, it.Next())
	expectedErr := errors.New("time encoding scheme for time unit 9 doesn't exist")
	require.Equal(t, expectedErr, it.Err())
}
