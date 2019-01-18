package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalc(t *testing.T) {
	vals := Float64Values{20.5, 13, 74, 98, 12, 76.4, 264, 201, 98, 30, 16, 12}
	a := Calc(vals)
	assert.Equal(t, Statistics{
		Min:    12,
		Max:    264,
		Mean:   76.24166666666666,
		Count:  12,
		Sum:    914.9,
		StdDev: 81.4067839292093,
	}, a)
}

func TestMergeAggregations(t *testing.T) {
	statistics := Merge([]Statistics{
		Calc(Float64Values{20.5, 13, 74, 98}),
		Calc(Float64Values{12, 76.4, 264, 201}),
		Calc(Float64Values{98, 30, 16, 12}),
		Statistics{
			Count: 0,
		},
	})
	assert.Equal(t, Statistics{
		Min:    12,
		Max:    264,
		Mean:   76.24166666666666,
		Count:  12,
		Sum:    914.9,
		StdDev: 86.30370872036085,
	}, statistics)
}

func TestMergeEmptyAggregations(t *testing.T) {
	statistics := Merge([]Statistics{
		Statistics{Count: 0},
		Statistics{Count: 0},
	})
	assert.Equal(t, Statistics{}, statistics)
}
