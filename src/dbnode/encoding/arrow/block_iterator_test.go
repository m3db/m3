package arrow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBlockIterator(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	s := start.UnixNano()
	it := newBlockIterator(s, int(time.Second*5), int(time.Minute))

	assert.Equal(t, 12, it.remaining())
	for i := 0; it.next(); i++ {
		dp := it.current()
		ts := start.Add(time.Second * time.Duration(i*5)).UnixNano()
		assert.Equal(t, ts, dp.ts)
		assert.Equal(t, float64(i), dp.val)
	}
}
