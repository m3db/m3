package resend

import (
	"math"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
)

const resendBufferLimit = time.Minute

// ResendBuffer is a fixed-size buffer for servicing resends of min and max
// aggregations.
type ResendBuffer interface {
	// Insert inserts a value into the buffer.
	//
	//  For a max buffer of size `k`, this will capture the `k` largest elements
	//  inserted into the buffer.
	//
	//  For a min buffer of size `k`, this will capture the `k` smallest elements
	//  inserted into the buffer.
	Insert(val float64)

	// Value returns the value for the buffer.
	//
	//  For a max buffer this will return the max value seen.
	//
	//  For a min buffer this will return the min value seen.
	Value() float64

	// Update updates a given value in the buffer, if eligible,
	// Regardless of buffer type, if `prevVal` currently appears in the buffer,
	// it is replaced by `newVal`.
	//
	//  For a max buffer, if a currently captured value is smaller than `newVal`,
	//  `prevVal` does not exist in the buffer, and the buffer is at capacity,
	//  the smallest currently captured value is replaced by newVal.
	//
	//  For a min buffer, if a currently captured value is larger than `newVal`,
	//  `prevVal` does not exist in the buffer, and the buffer is at capacity,
	//  the largest currently captured value is replaced by newVal.
	Update(prevVal float64, newVal float64)

	// Close closes the buffer.
	Close()
}

// ResendMetrics are metrics for resend buffers.
type ResendMetrics struct {
	// count is the total count of all created resend buffers.
	count tally.Counter
	// inserts is the total number of fresh inserts across all resend buffers.
	inserts tally.Counter
	// updates is he total number of resends across all resend buffers.
	updates tally.Counter

	// updatesPersisted is a
	updatesPersisted tally.Histogram

	// The bufferlimit is the size for the buffers.
	bufferLimit tally.Gauge
}

type resendBuffer struct {
	metrics          *ResendMetrics
	updatesPersisted float64
	comparisonFn     comparisonFn
	list             []float64
}

// NewMaxResendBufferMetrics builds resend metrics for the max buffer.
func NewMaxResendBufferMetrics(size int, iOpts instrument.Options) *ResendMetrics {
	scope := iOpts.MetricsScope().SubScope("resend").
		Tagged(map[string]string{"type": "max"})
	return newResendBufferMetrics(size, scope)
}

// NewMinResendBufferMetrics builds resend metrics for the min buffer.
func NewMinResendBufferMetrics(size int, iOpts instrument.Options) *ResendMetrics {
	scope := iOpts.MetricsScope().SubScope("resend").
		Tagged(map[string]string{"type": "min"})
	return newResendBufferMetrics(size, scope)
}

func newResendBufferMetrics(size int, scope tally.Scope) *ResendMetrics {
	updateBuckets := make(tally.ValueBuckets, 0, size)
	for i := 1; i <= size; i++ {
		// Add the bucket with an epsilon; these will always be reported as an
		// integer count, so adding an epsilon here will ensure we record into the
		// correct bucket.
		updateBuckets = append(updateBuckets, float64(i)+0.00001)
	}

	m := &ResendMetrics{
		count:   scope.Counter("count"),
		inserts: scope.Counter("inserted"),
		updates: scope.Counter("updated"),

		updatesPersisted: scope.Histogram("persisted]", updateBuckets),

		bufferLimit: scope.Gauge("buffer_limit"),
	}

	// Start reporting loop for reporting the buffer size limit.
	timer := time.NewTimer(resendBufferLimit)
	go func() {
		bufferLimit := float64(size)
		for {
			<-timer.C
			m.bufferLimit.Update(bufferLimit)
		}
	}()

	return m
}

type comparisonFn func(a, b float64) bool

func min(a, b float64) bool {
	if math.IsNaN(a) {
		return false
	}
	if math.IsNaN(b) {
		return true
	}
	return a < b
}

func max(a, b float64) bool {
	if math.IsNaN(a) {
		return false
	}
	if math.IsNaN(b) {
		return true
	}
	return a > b
}

// NewMaxBuffer returns a ResendBuffer that will keep up to  `k` max elements.
func NewMaxBuffer(k int, metrics *ResendMetrics) ResendBuffer {
	return newResendBuffer(k, max, metrics)
}

// NewMinBuffer returns a ResendBuffer that will keep up to `k` max elements.
func NewMinBuffer(k int, metrics *ResendMetrics) ResendBuffer {
	return newResendBuffer(k, min, metrics)
}

func newResendBuffer(
	k int,
	comparisonFn comparisonFn,
	metrics *ResendMetrics,
) ResendBuffer {
	metrics.count.Inc(1)

	return &resendBuffer{
		metrics:      metrics,
		comparisonFn: comparisonFn,

		// TODO: pooling.
		list: make([]float64, 0, k),
	}
}

func (b *resendBuffer) Insert(val float64) {
	b.metrics.inserts.Inc(1)

	// if list not full yet, fill it up.
	if len(b.list) < cap(b.list) {
		b.list = append(b.list, val)
		return
	}

	toUpdateVal := b.list[0]
	toUpdateIdx := 0

	for idx, listVal := range b.list {
		// find the best candidate to replace with the new value
		if b.comparisonFn(toUpdateVal, listVal) {
			toUpdateVal = listVal
			toUpdateIdx = idx
		}
	}

	// if the current value is a better candidate than the value to replace,
	// update it.
	if b.comparisonFn(val, toUpdateVal) {
		b.list[toUpdateIdx] = val
	}
}

func (b *resendBuffer) Value() float64 {
	if len(b.list) == 0 {
		return math.NaN()
	}

	toReturn := b.list[0]
	for _, val := range b.list[1:] {
		if b.comparisonFn(val, toReturn) {
			toReturn = val
		}
	}

	return toReturn
}

func (b *resendBuffer) Update(prevVal float64, newVal float64) {
	if len(b.list) == 0 {
		// received a resend before recording any values, which is an invalid case.
		return
	}

	b.metrics.updates.Inc(1)

	toUpdateVal := b.list[0]
	toUpdateIdx := 0

	for idx, listVal := range b.list {
		// found the previously recorded value in the list. Update and shortcircuit.
		if listVal == prevVal {
			b.list[idx] = newVal
			b.updatesPersisted++
			b.metrics.updatesPersisted.RecordValue(b.updatesPersisted)
			return
		}

		if b.comparisonFn(toUpdateVal, listVal) {
			toUpdateVal = listVal
			toUpdateIdx = idx
		}
	}

	// newVal is a better candidate than an existing value in the buffer.
	// Replace the least viable candidate in the buffer value with the new value.
	// This is only possible if the buffer is full; otherwise we are trying
	// to update a value which SHOULD be in the list, which is an invalid case.
	if len(b.list) == cap(b.list) && b.comparisonFn(newVal, toUpdateVal) {
		b.list[toUpdateIdx] = newVal
		b.updatesPersisted++
		b.metrics.updatesPersisted.RecordValue(b.updatesPersisted)
	}
}

func (b *resendBuffer) Close() {
	b.list = b.list[:0]
	// TODO: return buffer to pool.
}
