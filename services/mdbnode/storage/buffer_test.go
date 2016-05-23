package storage

import (
	"bytes"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	"code.uber.internal/infra/memtsdb/encoding/tsz"

	"github.com/stretchr/testify/assert"
)

type testFlusher struct {
	fn databaseBufferFlushFn
}

func (f testFlusher) onFlush(flush databaseBufferFlush) {
	f.fn(flush)
}

func testBufferDatabaseOptions() DatabaseOptions {
	newEncoderFn := func(start time.Time) encoding.Encoder {
		// TODO(r): encoder/decoder will not need unit
		return tsz.NewEncoder(start, time.Second)
	}
	newDecoderFn := func() encoding.Decoder {
		// TODO(r): encoder/decoder will not need unit
		return tsz.NewDecoder(time.Second)
	}
	return NewDatabaseOptions().
		BlockSize(2 * time.Minute).
		BufferResolution(1 * time.Second).
		NewEncoderFn(newEncoderFn).
		NewDecoderFn(newDecoderFn).
		NowFn(time.Now).
		BufferFuture(10 * time.Second).
		BufferPast(10 * time.Second).
		BufferFlush(5 * time.Second)
}

type value struct {
	timestamp  time.Time
	value      float64
	unit       time.Duration
	annotation []byte
}

type decodedValue struct {
	timestamp  time.Time
	value      float64
	annotation []byte
}

func decodedValues(iter encoding.Iterator) ([]decodedValue, error) {
	var values []decodedValue
	for iter.Next() {
		dp, annotation := iter.Current()
		values = append(values, decodedValue{dp.Timestamp, dp.Value, annotation})
	}
	return values, iter.Err()
}

func TestBufferWriteRead(t *testing.T) {
	opts := testBufferDatabaseOptions()

	curr := time.Now()
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)
	buffer.nowFn = func() time.Time {
		return curr
	}

	data := []value{
		// Make second resolution by truncating
		{curr.Add(-3 * time.Second).Truncate(time.Second), 1, time.Second, nil},
		{curr.Add(-2 * time.Second).Truncate(time.Second), 2, time.Second, nil},
		{curr.Add(-1 * time.Second).Truncate(time.Second), 3, time.Second, nil},
	}

	for _, v := range data {
		assert.NoError(t, buffer.write(v.timestamp, v.value, time.Second, v.annotation))
	}

	result := buffer.fetchEncodedSegment(curr.Add(-1*opts.GetBufferPast()), curr.Add(opts.GetBufferFuture()))
	assert.NotNil(t, result)

	newDecoderFn := opts.GetNewDecoderFn()
	values, err := decodedValues(newDecoderFn().Decode(bytes.NewBuffer(result)))
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	for i := 0; i < len(values); i++ {
		assert.True(t, data[i].timestamp.Equal(values[i].timestamp))
		assert.Equal(t, data[i].value, values[i].value)
		assert.Equal(t, data[i].annotation, values[i].annotation)
	}

}
