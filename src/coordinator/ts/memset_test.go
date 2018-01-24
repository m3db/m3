package ts

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemsetValues(t *testing.T) {
	ctx := context.TODO()
	values := newValues(ctx, 1000, 10000, 1)
	for i := 0; i < values.Len(); i++ {
		assert.InDelta(t, values.ValueAt(i), 1, 0.00000001)
	}
}

func TestMemsetZeroValues(t *testing.T) {
	ctx := context.TODO()
	values := newValues(ctx, 1000, 10000, 0)
	assert.InDelta(t, values.ValueAt(0), 0, 0.00000001)
}

func setValues(values []float64, initialValue float64) {
	for i := 0; i < len(values); i++ {
		values[i] = initialValue
	}
}
func BenchmarkMemsetZeroValues(b *testing.B) {
	values := make([]float64, 10000)
	for i := 0; i < b.N; i++ {
		Memset(values, 0)
	}

}

func BenchmarkLoopZeroValues(b *testing.B) {
	values := make([]float64, 10000)
	for i := 0; i < b.N; i++ {
		setValues(values, 0)
	}
}

func BenchmarkMemsetNonZeroValues(b *testing.B) {
	values := make([]float64, 10000)
	for i := 0; i < b.N; i++ {
		Memset(values, 1)
	}

}

func BenchmarkLoopNonZeroValues(b *testing.B) {
	values := make([]float64, 10000)
	for i := 0; i < b.N; i++ {
		setValues(values, 1)
	}
}
