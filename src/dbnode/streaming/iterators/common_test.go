package iterators

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ BaseIterator = (*sliceBasedIterator)(nil)

type sliceBasedIterator struct {
	slice     []iteratorSample
	position  int
	wasClosed bool
}

func newSliceBasedIterator(slice []iteratorSample) *sliceBasedIterator {
	return &sliceBasedIterator{slice: slice, position: -1}
}

func (it *sliceBasedIterator) Next() bool {
	it.position++
	return it.position < len(it.slice)
}

func (it *sliceBasedIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	value := it.slice[it.position]
	return value.dp, value.unit, value.annotation
}

func (it *sliceBasedIterator) Err() error {
	return nil
}

func (it *sliceBasedIterator) Close() {
	it.wasClosed = true
}

func (it *sliceBasedIterator) Clone() BaseIterator {
	cloned := *it

	return &cloned
}

func datapoint(time time.Time, value float64) ts.Datapoint {
	return ts.Datapoint{
		Timestamp:      time,
		TimestampNanos: xtime.ToUnixNano(time),
		Value:          value,
	}
}

func assertIteratorsEqual(t *testing.T, wanted BaseIterator, output BaseIterator) {
	for output.Next() {
		gotDp, gotUnits, gotAnnotation := output.Current()

		require.True(t, wanted.Next(), "Actual result was longer then wanted.")
		wantedDp, wantedUnits, wantedAnnotation := wanted.Current()

		assert.Equal(t, wantedDp, gotDp)
		assert.Equal(t, wantedUnits, gotUnits)
		assert.Equal(t, wantedAnnotation, gotAnnotation)
	}

	assert.False(t, wanted.Next(), "Actual result was shorter than wanted.")
	assert.NoError(t, output.Err())
	wanted.Close()
	output.Close()
}

func assertExhaustedAndClosed(t *testing.T, it *sliceBasedIterator) {
	assert.False(t, it.Next(), "Input iterator was not exhausted.")
	assert.True(t, it.wasClosed, "Input iterator was not closed.")
}
