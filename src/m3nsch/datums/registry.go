package datums

type tsRegistry struct {
	currentIdx        int
	numPointsPerDatum int
	tsGenMap          map[int]TSGenFn
}

func (reg *tsRegistry) Size() int {
	return len(reg.tsGenMap)
}

func (reg *tsRegistry) Get(i int) SyntheticTimeSeries {
	sz := reg.Size()
	idx := i % sz
	if idx < 0 {
		idx = idx + sz
	}
	datum, err := NewSyntheticTimeSeris(idx, reg.numPointsPerDatum, reg.tsGenMap[idx])
	if err != nil {
		panic(err)
	}
	return datum
}

// NewDefaultRegistry returns a Registry with default timeseries generators
func NewDefaultRegistry(numPointsPerDatum int) Registry {
	reg := &tsRegistry{
		numPointsPerDatum: numPointsPerDatum,
		tsGenMap:          make(map[int]TSGenFn),
	}
	reg.init()
	return reg
}

func (reg *tsRegistry) init() {
	// identity datum
	reg.addGenFn(func(i int) float64 {
		return float64(i)
	})

	// square datum
	reg.addGenFn(func(i int) float64 {
		return float64(i * i)
	})

	// TODO(prateek): make this bigger
}

func (reg *tsRegistry) addGenFn(f TSGenFn) {
	idx := reg.currentIdx
	reg.tsGenMap[idx] = f
	reg.currentIdx++
}
