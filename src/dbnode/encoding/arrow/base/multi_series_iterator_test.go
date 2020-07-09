package base

import (
	"testing"
	"time"

	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
)

func TestMultiSeriesIterator(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	s := xtime.UnixNano(start.UnixNano())
	multiSeriesIter := NewMultiSeriesIterator(10, 2, s, int(time.Second*5), int(time.Minute))
	multiSeriesCount := 0
	assert.Equal(t, 10, multiSeriesIter.Remaining())
	for multiSeriesIter.Next() {
		multiSeriesCount++
		it := multiSeriesIter.Current()
		assert.Equal(t, 2, it.Remaining())
		for i := 0; it.Next(); i++ {
			blockIter, blockStart := it.Current()
			assert.Equal(t, 12, blockIter.Remaining())
			exStart := start.Add(time.Minute * time.Duration(i))
			assert.Equal(t, xtime.UnixNano(exStart.UnixNano()), blockStart)
			for j := 0; blockIter.Next(); j++ {
				dp := blockIter.Current()
				ts := exStart.Add(time.Second * time.Duration(j*5)).UnixNano()
				assert.Equal(t, xtime.UnixNano(ts), dp.Timestamp)
				assert.Equal(t, float64(j), dp.Value)
			}
		}
	}

	assert.Equal(t, 10, multiSeriesCount)
}
