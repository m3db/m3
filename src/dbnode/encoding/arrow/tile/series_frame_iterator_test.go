package tile

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding/arrow/base"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/apache/arrow/go/arrow/math"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesFrameIterator(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	pool := memory.NewGoAllocator()

	recorder := newDatapointRecorder(pool)
	bl := base.NewBlockIterator(
		xtime.ToUnixNano(start),
		int(time.Second*10),
		int(time.Minute*5))

	it := newSeriesFrameIterator(
		xtime.ToUnixNano(start),
		xtime.UnixNano(time.Minute*2),
		recorder,
		bl)

	step := 0
	exSums := []float64{66 /* 0..11 */, 210 /* 12..23 */, 189 /* 24..30 */}
	counts := []int{12, 12, 7}

	exVal := 0.0
	exTime := start.UnixNano()
	for it.Next() {
		require.True(t, step < len(exSums))

		rec := it.Current()

		vals := rec.Values()
		assert.Equal(t, exSums[step], math.Float64.Sum(vals))
		require.Equal(t, counts[step], vals.Len())
		for i := 0; i < counts[step]; i++ {
			assert.Equal(t, exVal, vals.Value(i))
			exVal++
		}

		times := rec.Timestamps()
		require.Equal(t, counts[step], times.Len())
		for i := 0; i < counts[step]; i++ {
			assert.Equal(t, exTime, times.Value(i))
			exTime = exTime + int64(time.Second*10)
		}

		step++
	}
	assert.NoError(t, it.Close())
}
