package resend

import (
	"math"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
)

const resendBufferLimit = time.Minute

// ResendBuffer is a fixed-size buffer.
type ResendBuffer interface {
	// Insert inserts a value into the buffer.
	Insert(val float64)
	// Value returns the value for the buffer.
	Value() float64
	// Update updates a given value, if it appears in the buffer.
	Update(prevVal float64, newVal float64)
	// Close closes the buffer.
	Close()
}

// ResendMetrics are metrics for resend buffers.
type ResendMetrics struct {
	count   tally.Counter
	inserts tally.Counter
	updates tally.Counter

	updatesPersisted tally.Histogram

	bufferLimit tally.Gauge
}

type resendBuffer struct {
	metrics           *ResendMetrics
	updatesPersisted  float64
	isBetterCandidate compareFn
	isWorseCandidate  compareFn
	list              []float64
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

	// start reporting loop for resending the buffer limit.
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

// NewMaxBuffer returns a ResendBuffer that will keep the `size` max elements.
func NewMaxBuffer(size int, metrics *ResendMetrics) ResendBuffer {
	return newResendBuffer(size, max, metrics)
}

// NewMinBuffer returns a ResendBuffer that will keep the `size` max elements.
func NewMinBuffer(size int, metrics *ResendMetrics) ResendBuffer {
	return newResendBuffer(size, min, metrics)
}

func newResendBuffer(
	size int,
	compareFn compareFn,
	metrics *ResendMetrics,
) ResendBuffer {
	metrics.count.Inc(1)

	return &resendBuffer{
		metrics:   metrics,
		compareFn: compareFn,

		// TODO: pooling.
		list: make([]float64, 0, size),
	}
}

type compareFn func(a, b float64) bool

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

func (b *resendBuffer) Insert(val float64) {
	b.metrics.inserts.Inc(1)

	// if list not full yet, fill it up.
	if len(b.list) < cap(b.list) {
		b.list = append(b.list, val)
		return
	}

	min := b.list[0]
	minIdx := -1

	// if the incoming value compares, update at index 0.
	if val > min {
		minIdx = 0
	}

	for idx, elem := range b.list[1:] {
		// fmt.Println("Elem", elem, "min", min, "compare", b.compareFn(elem, min))
		if elem < min {
			min = elem
			if val > min {
				minIdx = idx + 1
			}
		}
	}

	if minIdx == -1 {
		return
	}

	b.list[minIdx] = val
}

func (b *resendBuffer) Value() float64 {
	if len(b.list) == 0 {
		return math.NaN()
	}

	toReturn := b.list[0]
	for _, val := range b.list[1:] {
		if b.compareFn(val, toReturn) {
			toReturn = val
		}
	}

	return toReturn
}

func (b *resendBuffer) Update(prevVal float64, newVal float64) {
	if len(b.list) == 0 {
		// we've received a resend before recording any values,
		// which is an invalid case.
		return
	}

	b.metrics.updates.Inc(1)

	minVal := b.list[0]
	minIdx := 0

	for idx, val := range b.list {
		if val == prevVal {
			b.list[idx] = newVal
			b.updatesPersisted++
			b.metrics.updatesPersisted.RecordValue(b.updatesPersisted)
			return
		}

		if minVal > val {
			minVal = val
			minIdx = idx
		}
	}

	// The value we're updating to is larger than an existing value in the buffer.
	// Replace the smallest previuosly seen value with the new value.
	// this is only possible if the buffer is full; otherwise we are trying
	// to update a value which SHOULD be in the list, which is an invalid case.
	if len(b.list) == cap(b.list) && minVal < newVal {
		b.list[minIdx] = newVal
		b.updatesPersisted++
		b.metrics.updatesPersisted.RecordValue(b.updatesPersisted)
	}
}

func (b *resendBuffer) Close() {
	b.list = b.list[:0]
	// TODO: return buffer to pool.
}
