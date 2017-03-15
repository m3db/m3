package datums

import "fmt"

var (
	errNumPointsNegative = fmt.Errorf("numPoints must be positive")
)

type synTS struct {
	id   int
	data []float64

	lastIdx int
}

func (ld *synTS) ID() int {
	return ld.id
}

func (ld *synTS) Size() int {
	return len(ld.data)
}

func (ld *synTS) Data() []float64 {
	return ld.data
}

func (ld *synTS) Get(idx int) float64 {
	idx = idx % len(ld.data)
	if idx < 0 {
		idx += len(ld.data)
	}
	return ld.data[idx]
}

func (ld *synTS) Next() float64 {
	idx := (ld.lastIdx + 1) % len(ld.data)
	if idx < 0 {
		idx += len(ld.data)
	}
	ld.lastIdx = idx
	return ld.data[idx]
}

// NewSyntheticTimeSeris generates a new SyntheticTimeSeris using the provided parameters.
func NewSyntheticTimeSeris(id int, numPoints int, fn TSGenFn) (SyntheticTimeSeries, error) {
	if numPoints < 0 {
		return nil, errNumPointsNegative
	}
	data := make([]float64, numPoints)
	for i := 0; i < numPoints; i++ {
		data[i] = fn(i)
	}
	return &synTS{
		id:      id,
		data:    data,
		lastIdx: -1,
	}, nil
}
