package tile

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/apache/arrow/go/arrow/math"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSequentialIterator(
	ctrl *gomock.Controller,
	start time.Time,
	step time.Duration,
	numPoints int,
) encoding.ReaderIterator {
	bl := encoding.NewMockReaderIterator(ctrl)
	currVal, currTs, currTsNano := 0.0, start, xtime.ToUnixNano(start)
	for i := 0; i < numPoints; i++ {
		i := i
		bl.EXPECT().Next().DoAndReturn(func() bool {
			// NB: only increment after first Next.
			if i > 0 {
				currVal++
				currTs = currTs.Add(step)
				currTsNano += xtime.UnixNano(step)
			}
			return true
		}).Times(1)

		bl.EXPECT().Current().DoAndReturn(func() (ts.Datapoint, xtime.Unit, []byte) {
			return ts.Datapoint{
				Value:          currVal,
				Timestamp:      currTs,
				TimestampNanos: currTsNano,
			}, xtime.Second, nil
		}).AnyTimes()
	}

	bl.EXPECT().Next().Return(false)
	bl.EXPECT().Err().Return(nil).AnyTimes()
	bl.EXPECT().Close()

	return bl
}

func halfFrameSizes(numPoints int) []float64 {
	frames := make([]float64, numPoints*2-1)
	v := 0.0
	for i := range frames {
		if i%2 == 0 {
			frames[i] = v
			v++
		}
	}

	return frames
}

func halfFrameCounts(numPoints int) []int {
	frames := make([]int, numPoints*2-1)
	for i := range frames {
		if i%2 == 0 {
			frames[i] = 1
		}
	}

	return frames
}

func TestSeriesFrameIterator(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	numPoints := 30
	start := time.Now().Truncate(time.Hour)
	pool := memory.NewGoAllocator()

	stepSize := time.Second * 10

	tests := []struct {
		name      string
		frameSize time.Duration
		exCounts  []int
		exSums    []float64
	}{
		{
			name:      "5 second frame, 1 point every 2 frames - with empty frames",
			frameSize: time.Second * 5,
			exSums:    halfFrameSizes(numPoints),
			exCounts:  halfFrameCounts(numPoints),
		},
		{
			name:      "1 minute frame, 6 points per frame",
			frameSize: time.Minute * 1,
			exSums: []float64{
				15 /* Σ 0..5 */, 51 /* Σ 6..11 */, 87, /* Σ 12..17 */
				123 /*Σ18..23 */, 159 /*Σ24..30 */},
			exCounts: []int{6, 6, 6, 6, 6},
		},
		{
			frameSize: time.Minute * 2,
			exSums:    []float64{66 /* Σ 0..11 */, 210 /* Σ 12..23 */, 159 /*Σ24..30 */},
			exCounts:  []int{12, 12, 6},
		},
		{
			name:      "3 minute frame, 18 points per frame",
			frameSize: time.Minute * 3,
			exSums:    []float64{153 /*Σ0..17 */, 282 /* Σ 18..30 */},
			exCounts:  []int{18, 12},
		},
		{
			name:      "4 minute frame, 24 points per frame",
			frameSize: time.Minute * 4,
			exSums:    []float64{276 /*Σ0..23 */, 159 /* Σ 24..30 */},
			exCounts:  []int{24, 6},
		},
		{
			name:      "5 minute frame, 30 points per frame",
			frameSize: time.Minute * 5,
			exSums:    []float64{435},
			exCounts:  []int{30},
		},
		{
			name:      "6 minute frame, 30 points per frame (exhausted)",
			frameSize: time.Minute * 6,
			exSums:    []float64{435},
			exCounts:  []int{30},
		},
	}

	recorder := newDatapointRecorder(pool)
	it := newSeriesFrameIterator(recorder)
	require.False(t, it.Next())
	require.Error(t, it.Err())

	for _, tt := range tests {
		bl := newSequentialIterator(ctrl, start, stepSize, numPoints)
		require.NoError(t, it.Reset(
			xtime.ToUnixNano(start),
			xtime.UnixNano(tt.frameSize),
			bl,
		))

		step := 0
		exVal := 0.0
		exTime := start.UnixNano()
		for it.Next() {
			require.True(t, step < len(tt.exSums))
			rec := it.Current()
			assert.NotNil(t, rec)

			vals := rec.Values()
			assert.Equal(t, tt.exSums[step], math.Float64.Sum(vals))
			require.Equal(t, tt.exCounts[step], vals.Len())
			for i := 0; i < tt.exCounts[step]; i++ {
				assert.Equal(t, exVal, vals.Value(i))
				exVal++
			}

			times := rec.Timestamps()
			require.Equal(t, tt.exCounts[step], times.Len())
			for i := 0; i < tt.exCounts[step]; i++ {
				assert.Equal(t, exTime, times.Value(i))
				exTime = exTime + int64(time.Second*10)
			}

			step++
		}
		assert.Equal(t, len(tt.exSums), step)
		assert.NoError(t, it.Err())
	}
	assert.NoError(t, it.Close())
}
