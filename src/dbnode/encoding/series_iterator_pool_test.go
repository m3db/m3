package encoding

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestSeriesIteratorPool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewSeriesIteratorPool(nil)
	pool.Init()

	iter := pool.Get()
	assert.NotNil(t, iter)

	pool.Put(iter)
}
