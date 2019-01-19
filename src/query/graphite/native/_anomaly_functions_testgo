package native

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runFiveSigma(ctx *common.Context, readings []float64) (*ts.Series, error) {
	start, _ := time.Parse(time.RFC3339, "2001-01-01T12:00:00Z")
	millisInMin := int(time.Minute.Seconds() * 1000.0)
	values := common.NewTestSeriesValues(ctx, millisInMin, readings)
	series := ts.NewSeries(ctx, "stats.sjc1.foo", start, values)
	labels, err := fiveSigma(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	})
	if err != nil {
		return nil, err
	}
	return labels.Values[0], nil
}

func TestFiveSigmaInputValidation(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	_, err := fiveSigma(ctx, singlePathSpec{
		Values: nil,
	})
	assert.NotNil(t, err)

	_, err = fiveSigma(ctx, singlePathSpec{
		Values: []*ts.Series{},
	})
	assert.NotNil(t, err)
}

func TestFiveSigmaLabelsNormalPoints(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	labels, err := runFiveSigma(ctx, []float64{10, 9, 11, 10, 9, 11, 10, 9, 11, 10})
	require.Nil(t, err)

	assert.Equal(t, labels.Len(), 10)
	for i := 0; i < labels.Len(); i++ {
		assert.InEpsilon(t, labels.ValueAt(i), 0, 0.0001)
	}
}

func TestFiveSigmaLabelsLowPoints(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	// For first 9 values, mean is 10 and sigma is ~0.87
	labels, err := runFiveSigma(ctx, []float64{10, 9, 11, 10, 9, 11, 10, 9, 11, 5})
	require.Nil(t, err)
	assert.InEpsilon(t, labels.ValueAt(9), -1, 0.0001)
}

func TestFiveSigmaLabelsHighPoints(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	// For first 9 values, mean is 10 and sigma is ~0.87
	labels, err := runFiveSigma(ctx, []float64{10, 9, 11, 10, 9, 11, 10, 9, 11, 15})
	require.Nil(t, err)
	assert.InEpsilon(t, labels.ValueAt(9), 1, 0.0001)
}
