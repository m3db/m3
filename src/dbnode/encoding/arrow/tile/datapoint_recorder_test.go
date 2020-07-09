package tile

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/apache/arrow/go/arrow/math"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDatapointRecorder(t *testing.T) {
	pool := memory.NewGoAllocator()
	recorder := NewDatapointRecorder(pool)

	addPoints := func(size int) {
		for i := 0; i < size; i++ {
			recorder.appendPoints(ts.Datapoint{
				Value:          float64(i),
				TimestampNanos: xtime.UnixNano(i),
			})
		}
	}

	verify := func(rec *datapointRecord, size int) {
		ex := float64(size*(size-1)) / 2
		vals := rec.values()
		assert.Equal(t, ex, math.Float64.Sum(vals))

		require.Equal(t, size, vals.Len())
		for i := 0; i < size; i++ {
			assert.Equal(t, float64(i), vals.Value(i))
		}

		times := rec.timestamps()
		require.Equal(t, size, times.Len())
		for i := 0; i < size; i++ {
			assert.Equal(t, int64(i), times.Value(i))
		}
	}

	size := 10
	addPoints(size)

	rec := newDatapointRecord()
	recorder.updateRecord(rec)
	verify(rec, size)
	rec.release()

	size = 150000
	addPoints(size)
	recorder.updateRecord(rec)
	verify(rec, size)
	rec.release()
}
