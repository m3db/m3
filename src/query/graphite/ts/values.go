package ts

import (
	"math"
	"sort"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/stats"
	xts "github.com/m3db/m3/src/query/ts"

	xpool "github.com/m3db/m3x/pool"
)

// Values holds the values for a timeseries.  It provides a minimal interface
// for storing and retrieving values in the series, with Series providing a
// more convenient interface for applications to build on top of.  Values
// objects are not specific to a given time, allowing them to be
// pre-allocated, pooled, and re-used across multiple Series.  There are
// multiple implementations of Values so that we can optimize storage based on
// the density of the series.
type Values interface {
	stats.Values

	// The number of millisseconds represented by each index
	MillisPerStep() int

	// Slice of data values in a range
	Slice(begin, end int) Values

	// AllNaN returns true if the values are all NaN
	AllNaN() bool

	// TODO(mmihic): Calculate quantiles

	// TODO(mmihic): Bulk values interfaces
}

// MutableValues is the interface for values that can be updated
type MutableValues interface {
	Values

	// Resets the values
	Reset()

	// Sets the value at the given entry
	SetValueAt(n int, v float64)
}

// CustomStatistics are for values that do custom statistics calculations
type CustomStatistics interface {
	CalcStatistics() stats.Statistics
}

// NewConstantValues returns a block of timeseries values all of which have the
// same value
func NewConstantValues(ctx context.Context, value float64, numSteps, millisPerStep int) Values {
	return constantValues{
		numSteps:      numSteps,
		millisPerStep: millisPerStep,
		value:         value,
	}
}

type constantValues struct {
	numSteps      int
	millisPerStep int
	value         float64
}

func (values constantValues) AllNaN() bool              { return math.IsNaN(values.value) }
func (values constantValues) MillisPerStep() int        { return values.millisPerStep }
func (values constantValues) Len() int                  { return values.numSteps }
func (values constantValues) ValueAt(point int) float64 { return values.value }
func (values constantValues) Slice(begin, end int) Values {
	return &constantValues{
		end - begin,
		values.millisPerStep,
		values.value,
	}
}

func (values constantValues) CalcStatistics() stats.Statistics {
	if math.IsNaN(values.value) {
		return stats.Statistics{
			Count:  0,
			StdDev: 0,
			Min:    math.NaN(),
			Max:    math.NaN(),
			Mean:   math.NaN(),
		}
	}

	return stats.Statistics{
		Count:  uint(values.numSteps),
		Min:    values.value,
		Max:    values.value,
		Mean:   values.value,
		StdDev: 0,
	}
}

// NewZeroValues returns a MutableValues supporting the given number of values
// at the requested granularity.  The values start off initialized at 0
func NewZeroValues(ctx context.Context, millisPerStep, numSteps int) MutableValues {
	return newValues(ctx, millisPerStep, numSteps, 0)
}

// NewValues returns MutableValues supporting the given number of values at the
// requested granularity.  The values start off as NaN
func NewValues(ctx context.Context, millisPerStep, numSteps int) MutableValues {
	return newValues(ctx, millisPerStep, numSteps, math.NaN())
}

var (
	pooledValuesLength         = []int{}
	pooledConsolidationsLength = []int{}
)

var (
	timeSeriesValuesPools xpool.BucketizedObjectPool
	consolidationPools    xpool.BucketizedObjectPool
)

func findValuesPoolIndex(numPoints int) int {
	return sort.Search(len(pooledValuesLength), func(i int) bool {
		return pooledValuesLength[i] >= numPoints
	})
}

func findConsolidationPoolIndex(numPoints int) int {
	return sort.Search(len(pooledConsolidationsLength), func(i int) bool {
		return pooledConsolidationsLength[i] >= numPoints
	})
}

func newValues(ctx context.Context, millisPerStep, numSteps int, initialValue float64) MutableValues {
	var values []float64
	var pooled bool

	if timeSeriesValuesPools != nil {
		temp := timeSeriesValuesPools.Get(numSteps)
		values = temp.([]float64)
		if cap(values) >= numSteps {
			values = values[:numSteps]
			pooled = true
		}
	}

	if !pooled {
		values = make([]float64, numSteps)
	}

	// Faster way to initialize an array instead of a loop
	xts.Memset(values, initialValue)
	vals := &float64Values{
		ctx:           ctx,
		millisPerStep: millisPerStep,
		numSteps:      numSteps,
		allNaN:        math.IsNaN(initialValue),
		values:        values,
	}
	ctx.RegisterCloser(vals)
	return vals
}

type float64Values struct {
	ctx           context.Context
	millisPerStep int
	numSteps      int
	values        []float64
	allNaN        bool
}

func (b *float64Values) Reset() {
	for i := range b.values {
		b.values[i] = math.NaN()
	}
	b.allNaN = true
}

func (b *float64Values) Close() error {
	if timeSeriesValuesPools != nil {
		timeSeriesValuesPools.Put(b.values, cap(b.values))
	}
	b.numSteps = 0
	b.values = nil
	return nil
}

func (b *float64Values) AllNaN() bool              { return b.allNaN }
func (b *float64Values) MillisPerStep() int        { return b.millisPerStep }
func (b *float64Values) Len() int                  { return b.numSteps }
func (b *float64Values) ValueAt(point int) float64 { return b.values[point] }
func (b *float64Values) SetValueAt(point int, v float64) {
	b.allNaN = b.allNaN && math.IsNaN(v)
	b.values[point] = v
}

func (b *float64Values) Slice(begin, end int) Values {
	return &float64Values{
		ctx:           b.ctx,
		millisPerStep: b.millisPerStep,
		values:        b.values[begin:end],
		numSteps:      end - begin,
		allNaN:        false, // NB(mmihic): Someone might modify the parent and we won't be able to tell
	}
}

// PoolBucket is a pool bucket
type PoolBucket struct {
	Capacity int
	Count    int
}

func initPools(valueBuckets, consolidationBuckets []xpool.Bucket) error {
	pooledValuesLength = pooledValuesLength[:0]
	pooledConsolidationsLength = pooledConsolidationsLength[:0]

	for _, b := range valueBuckets {
		pooledValuesLength = append(pooledValuesLength, b.Capacity)
	}
	for _, b := range consolidationBuckets {
		pooledConsolidationsLength = append(pooledConsolidationsLength, b.Capacity)
	}

	poolOpts := xpool.NewObjectPoolOptions()
	valuesOpts := poolOpts.SetInstrumentOptions(
		poolOpts.InstrumentOptions())
	consolidationOpts := poolOpts.SetInstrumentOptions(
		poolOpts.InstrumentOptions())
	timeSeriesValuesPools = xpool.NewBucketizedObjectPool(valueBuckets, valuesOpts)
	timeSeriesValuesPools.Init(func(capacity int) interface{} {
		return make([]float64, capacity)
	})
	consolidationPools = xpool.NewBucketizedObjectPool(consolidationBuckets, consolidationOpts)
	consolidationPools.Init(func(capacity int) interface{} {
		return newConsolidation(capacity)
	})
	return nil
}

var gigabyteBytes = math.Pow(2, 30)

// EnablePooling enables pooling, measuring the impacts at the given scope
func EnablePooling(
	valueBuckets, consolidationBuckets []xpool.Bucket,
) {
	totalBytes := 0
	for _, buckets := range [][]xpool.Bucket{
		valueBuckets, consolidationBuckets,
	} {
		totalBytes += approxPoolValuesBytes(buckets)
	}
	//FIXMElog.Infof("Allocating approx %fGB for pooled values",
	// float64(totalBytes)/gigabyteBytes)
	if err := initPools(valueBuckets, consolidationBuckets); err != nil {
		// log.Errorf("Could not initialize timeSeriesValuesPool: %v", err)
	} else {
		// log.Infof("Done allocating pooled values")
	}
}

func approxPoolValuesBytes(buckets []xpool.Bucket) int {
	totalBytes := 0
	sizeValueBytes := 8
	for _, b := range buckets {
		totalBytes += b.Capacity * b.Count * sizeValueBytes
	}
	return totalBytes
}
