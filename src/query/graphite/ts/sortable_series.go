package ts

import (
	"math"
	"sort"
)

// Direction signifies ascending or descending order
type Direction int

const (
	// Ascending order
	Ascending Direction = iota
	// Descending order
	Descending
)

type sortableSeries struct {
	series *Series
	value  float64
}

type sortableSeriesList []sortableSeries

func (s sortableSeriesList) Len() int { return len(s) }

func (s sortableSeriesList) Less(i, j int) bool {
	if math.IsNaN(s[i].value) && !math.IsNaN(s[j].value) {
		return true
	}
	return s[i].value < s[j].value
}

func (s sortableSeriesList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// SortSeries applies a given SeriesReducer to each series in the input
// list and sorts based on the assigned value
func SortSeries(in []*Series, sr SeriesReducer, dir Direction) ([]*Series, error) {
	var (
		sortableList = make(sortableSeriesList, 0, len(in))
		results      = make([]*Series, len(in))
	)

	for _, series := range in {
		sortableList = append(sortableList, sortableSeries{series: series, value: sr(series)})
	}

	if dir == Ascending {
		sort.Sort(sortableList)
	} else {
		sort.Sort(sort.Reverse(sortableList))
	}

	for i, sortable := range sortableList {
		results[i] = sortable.series
	}

	return results, nil
}
