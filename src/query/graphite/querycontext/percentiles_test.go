package querycontext

import (
	"testing"

	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
)

type percentileFunction func(ctx *Context, seriesList []*ts.Series, percentile float64) ([]*ts.Series, error)

type percentileTestParams struct {
	interpolate bool
	percentile  float64
	input       []float64
	expected    float64
}

func TestGetPercentile(t *testing.T) {
	tests := []percentileTestParams{
		{
			false,
			0,
			[]float64{1, 2, 3, 4, 5},
			1,
		},
		{
			false,
			10,
			[]float64{1, 2, 3, 4, 5},
			1,
		},
		{
			false,
			50,
			[]float64{1, 2, 3, 4, 5},
			3,
		},
		{
			true,
			50,
			[]float64{1, 2, 3, 4, 5},
			2.5,
		},
		{
			false,
			50,
			[]float64{1, 2, 3, 4, 5, 6},
			3,
		},
		{
			true,
			50,
			[]float64{1, 2, 3, 4, 5, 6},
			3,
		},
		{
			false,
			90,
			[]float64{1, 2, 3, 4, 5},
			5,
		},
		{
			false,
			50,
			[]float64{1},
			1,
		},
		{
			false,
			50,
			[]float64{1, 2},
			1,
		},
		{
			true,
			30,
			[]float64{32, 34, 62, 73, 75},
			33,
		},
		{
			true,
			33,
			[]float64{32, 34, 73, 75},
			32.64,
		},
	}

	for _, test := range tests {
		testGetPercentile(t, test)
	}
}

func testGetPercentile(t *testing.T, test percentileTestParams) {
	actual := GetPercentile(test.input, test.percentile, test.interpolate)
	assert.Equal(t, test.expected, actual)
}
