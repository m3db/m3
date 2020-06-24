package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBlockIterator(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	s := start.UnixNano()
	it := NewBlockIterator(s, int(time.Second*5), int(time.Minute))

	assert.Equal(t, 12, it.Remaining())
	for i := 0; it.Next(); i++ {
		dp := it.Current()
		ts := start.Add(time.Second * time.Duration(i*5)).UnixNano()
		assert.Equal(t, ts, dp.Timestamp)
		assert.Equal(t, float64(i), dp.Value)
	}
}
