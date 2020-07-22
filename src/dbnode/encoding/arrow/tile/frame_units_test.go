package tile

import (
	"testing"

	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesFrameUnitsSingle(t *testing.T) {
	rec := newUnitRecorder()

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	rec.record(xtime.Second)
	v, ok := rec.SingleValue()
	assert.True(t, ok)
	assert.Equal(t, xtime.Second, v)

	rec.record(xtime.Second)
	v, ok = rec.SingleValue()
	assert.True(t, ok)
	assert.Equal(t, xtime.Second, v)

	vals := rec.Values()
	assert.Equal(t, []xtime.Unit{xtime.Second, xtime.Second}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)
}

func TestSeriesFrameUnitsMultiple(t *testing.T) {
	rec := newUnitRecorder()
	rec.record(xtime.Second)
	rec.record(xtime.Second)
	rec.record(xtime.Day)

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	vals := rec.Values()
	assert.Equal(t, []xtime.Unit{xtime.Second, xtime.Second, xtime.Day}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)

	vals = rec.Values()
	require.Equal(t, 0, len(vals))
}

func TestSeriesFrameUnitsMultipleChanges(t *testing.T) {
	rec := newUnitRecorder()
	rec.record(xtime.Second)
	rec.record(xtime.Day)
	rec.record(xtime.Nanosecond)

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	vals := rec.Values()
	assert.Equal(t, []xtime.Unit{xtime.Second, xtime.Day, xtime.Nanosecond}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)

	vals = rec.Values()
	require.Equal(t, 0, len(vals))
}
