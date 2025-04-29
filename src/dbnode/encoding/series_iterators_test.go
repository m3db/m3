package encoding

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestSeriesIterators(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock iterators using GoMock
	iter1 := NewMockSeriesIterator(ctrl)
	iter2 := NewMockSeriesIterator(ctrl)

	iter1.EXPECT().Close().Times(1)
	iter2.EXPECT().Close().Times(1)

	// Test NewSeriesIterators
	iters := NewSeriesIterators([]SeriesIterator{iter1, iter2})
	assert.Equal(t, 2, iters.Len())
	assert.Equal(t, []SeriesIterator{iter1, iter2}, iters.Iters())

	// Test SetAt
	mockIter := NewMockSeriesIterator(ctrl)
	iters.SetAt(1, mockIter)
	assert.Equal(t, mockIter, iters.Iters()[1])

	// Test Reset
	iters.Reset(1)
	assert.Equal(t, 1, iters.Len())
	assert.Nil(t, iters.Iters()[0]) // Reset should nil out values

	// Test NewSizedSeriesIterators
	sizedIters := NewSizedSeriesIterators(2)
	assert.Equal(t, 2, sizedIters.Len())
	sizedIters.SetAt(0, iter1)
	sizedIters.SetAt(1, iter2)

	// Test Close
	sizedIters.Close()
	assert.Nil(t, sizedIters.Iters()[0])

	// Test EmptySeriesIterators
	assert.Equal(t, 0, EmptySeriesIterators.Len())
	assert.Nil(t, EmptySeriesIterators.Iters())
	EmptySeriesIterators.Close() // Should do nothing
}
