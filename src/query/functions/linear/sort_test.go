package linear

import (
	"math"
	"sort"
	"testing"

	"github.com/m3db/m3/src/query/functions/utils"
	"github.com/m3db/m3/src/query/test"

	"github.com/stretchr/testify/require"
)

func TestShouldFailWhenOpTypeIsInvalid(t *testing.T) {
	_, err := NewSortOp("sortAsc")
	require.Error(t, err)
}

func TestAscSort(t *testing.T) {
	actual := []float64{ 5.0, 4.1, math.NaN(), 8.6, 0.1 }
	expected := []float64{ 0.1, 4.1, 5.0, 8.6, math.NaN() }

	sort.Slice(actual, func(i, j int) bool {
		return utils.AscFloat64(actual[i], actual[j])
	})

	test.EqualsWithNans(t, expected, actual)
}

func TestDescSort(t *testing.T) {
	actual := []float64{ 5.0, 4.1, math.NaN(), 8.6, 0.1 }
	expected := []float64{ 8.6, 5.0, 4.1, 0.1, math.NaN() }

	sort.Slice(actual, func(i, j int) bool {
		return utils.DescFloat64(actual[i], actual[j])
	})

	test.EqualsWithNans(t, expected, actual)
}