package encoding

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestSeriesIteratorPool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock object pool
	pool := NewSeriesIteratorPool(nil)
	pool.Init()

	// Test Get()
	iter := pool.Get()
	assert.NotNil(t, iter)

	// Test Put()
	pool.Put(iter)
}
