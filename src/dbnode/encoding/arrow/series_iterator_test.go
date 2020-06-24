package arrow

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSeriesIterator(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	s := start.UnixNano()
	it := newSeriesIterator(4, s, int(time.Second*5), int(time.Minute))

	assert.Equal(t, 4, it.remaining())
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
