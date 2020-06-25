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

func TestResultsIterator(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	pool := memory.NewGoAllocator()

	recorder := newDatapointRecorder(pool)
	bl := base.NewMultiSeriesIterator(8, 3, xtime.ToUnixNano(start),
		int(time.Second*10), int(time.Minute*5))
	multiSeriesIter := newResultsIterator(xtime.ToUnixNano(start),
		xtime.UnixNano(time.Minute*2), recorder, bl)

	series := 0
	for multiSeriesIter.Next() {
		series++
		seriesIter := multiSeriesIter.Current()
		blocks := 0
		exTime := start.UnixNano()
		for seriesIter.Next() {
			blocks++
			it := seriesIter.Current()
			step := 0
			exSums := []float64{66 /* 0..11 */, 210 /* 12..23 */, 189 /* 24..30 */}
			counts := []int{12, 12, 7}

			exVal := 0.0
			for it.Next() {
				require.True(t, step < len(exSums))

				rec := it.Current()

				vals := rec.Values()
				require.NotNil(t, vals)

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

			// NB: test construction is a bit messy, so points across
			// this boundary "overlap" but that doens't affect the behavior under test.
			exTime = exTime - int64(time.Second*10)
		}
	}

	assert.NoError(t, multiSeriesIter.Close())
}
