package storage

import (
	"io"
	"sort"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"github.com/stretchr/testify/assert"
)

var timeDistantFuture = time.Now().Add(10 * 365 * 24 * time.Hour)

func secs(x float64) time.Duration {
	return time.Duration(x * float64(time.Second))
}

func mins(x float64) time.Duration {
	return time.Duration(x * float64(time.Minute))
}

func hrs(x float64) time.Duration {
	return time.Duration(x * float64(time.Hour))
}

type flush struct {
	start   time.Time
	encoder memtsdb.Encoder
}

type value struct {
	timestamp  time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
}

type valuesByTime []value

func (v valuesByTime) Len() int {
	return len(v)
}

func (v valuesByTime) Less(lhs, rhs int) bool {
	return v[lhs].timestamp.Before(v[rhs].timestamp)
}

func (v valuesByTime) Swap(lhs, rhs int) {
	v[lhs], v[rhs] = v[rhs], v[lhs]
}

type decodedValue struct {
	timestamp  time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
}

// Implements sort.Interface
type decodedValuesByTime []decodedValue

func (v decodedValuesByTime) Len() int {
	return len(v)
}

func (v decodedValuesByTime) Less(lhs, rhs int) bool {
	return v[lhs].timestamp.Before(v[rhs].timestamp)
}

func (v decodedValuesByTime) Swap(lhs, rhs int) {
	v[lhs], v[rhs] = v[rhs], v[lhs]
}

func decodedValues(results []io.Reader, opts memtsdb.DatabaseOptions) ([]decodedValue, error) {
	var all []decodedValue
	for i := range results {
		newDecoderFn := opts.GetNewDecoderFn()
		iter := newDecoderFn().Decode(results[i])

		var values []decodedValue
		for iter.Next() {
			dp, unit, annotation := iter.Current()
			values = append(values, decodedValue{dp.Timestamp, dp.Value, unit, annotation})
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}

		all = append(all, values...)
	}
	return all, nil
}

func assertValuesEqual(t *testing.T, values []value, results []io.Reader, opts memtsdb.DatabaseOptions) {
	decodedValues, err := decodedValues(results, opts)

	// TODO(r): avoid sorting results after once reader created that can read back out of order in order
	sort.Sort(decodedValuesByTime(decodedValues))

	assert.NoError(t, err)
	assert.Len(t, decodedValues, len(values))
	for i := 0; i < len(decodedValues); i++ {
		assert.True(t, values[i].timestamp.Equal(decodedValues[i].timestamp))
		assert.Equal(t, values[i].value, decodedValues[i].value)
		assert.Equal(t, values[i].unit, decodedValues[i].unit)
		assert.Equal(t, values[i].annotation, decodedValues[i].annotation)
	}
}
