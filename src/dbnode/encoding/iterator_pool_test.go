package encoding

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/pool"
)

// Mocked allocation function
func mockReaderIteratorAllocate(_ xio.Reader64, _ namespace.SchemaDescr) ReaderIterator {
	ctrl := gomock.NewController(nil)
	defer ctrl.Finish()
	return NewMockReaderIterator(ctrl)
}

func TestReaderIteratorPool(t *testing.T) {
	opts := pool.NewObjectPoolOptions()
	pool := NewReaderIteratorPool(opts)

	// Initialize pool with mock allocator
	pool.Init(mockReaderIteratorAllocate)

	// Get a reader iterator from the pool
	iter := pool.Get()
	assert.NotNil(t, iter)

	// Return the iterator to the pool
	pool.Put(iter)
}

func TestMultiReaderIteratorPool(t *testing.T) {
	opts := pool.NewObjectPoolOptions()
	pool := NewMultiReaderIteratorPool(opts)

	// Initialize pool with mock allocator
	pool.Init(mockReaderIteratorAllocate)

	// Get a multi-reader iterator from the pool
	iter := pool.Get()
	assert.NotNil(t, iter)

	// Return the iterator to the pool
	pool.Put(iter)
}
