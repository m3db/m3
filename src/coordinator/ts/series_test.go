package ts

import (
	"context"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/models"

	"github.com/stretchr/testify/assert"
)

func TestCreateNewSeries(t *testing.T) {
	ctx := context.TODO()
	startTime := time.Now()
	tags := models.Tags{"foo": "bar", "biz": "baz"}
	values := newValues(ctx, 1000, 10000, 1)
	series := NewSeries(ctx, "metrics", startTime, values, tags)

	assert.Equal(t, "metrics", series.Name())
	assert.Equal(t, 10000, series.Len())
	assert.Equal(t, 1000, series.MillisPerStep())
	assert.Equal(t, 1.0, series.ValueAt(0))
	assert.Equal(t, startTime, series.StartTime())
	assert.Equal(t, startTime, series.StartTimeForStep(0))
}
