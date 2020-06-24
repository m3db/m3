package arrow

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMultiSeriesIterator(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	s := start.UnixNano()
	multiSeriesIter := newMultiSeriesIterator(10, 2, s, int(time.Second*5), int(time.Minute))
	multiSeriesCount := 0
	assert.Equal(t, 10, multiSeriesIter.remaining())
	for multiSeriesIter.next() {
		multiSeriesCount++
		it := multiSeriesIter.current()
		assert.Equal(t, 2, it.remaining())
		for i := 0; it.next(); i++ {
			blockIter, blockStart := it.current()
			assert.Equal(t, 12, blockIter.remaining())
			assert.Equal(t, start.Add(time.Minute*time.Duration(i)), blockStart)
			for j := 0; blockIter.next(); j++ {
				dp := blockIter.current()
				ts := start.
					Add(time.Minute * time.Duration(i)).
					Add(time.Second * time.Duration(j*5)).UnixNano()

				assert.Equal(t, ts, dp.ts, fmt.Sprint(i, j))
				assert.Equal(t, float64(j), dp.val)
			}
		}
	}

	assert.Equal(t, 10, multiSeriesCount)
}
