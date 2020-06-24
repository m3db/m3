package arrow

import (
	"testing"

	"github.com/apache/arrow/go/arrow/math"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestDatapointRecorder(t *testing.T) {
	pool := memory.NewGoAllocator()
	recorder := newDatapointRecorder(pool)

	addPoints := func(size int) {
		for i := 0; i < size; i++ {
			recorder.appendPoints(dp{
				val: float64(i),
				ts:  int64(i),
			})
		}
	}

	size := 10
	addPoints(size)
	rec := recorder.buildRecord()
	ex := float64(size*(size-1)) / 2
	assert.Equal(t, ex, math.Float64.Sum(rec.values()))

	rec.Release()

	size = 150000
	addPoints(size)
	rec = recorder.buildRecord()
	ex = float64(size*(size-1)) / 2
	assert.Equal(t, ex, math.Float64.Sum(rec.values()))

	rec.Release()
}
