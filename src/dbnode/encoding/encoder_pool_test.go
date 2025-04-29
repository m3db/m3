package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/x/pool"
)

func TestEncoderPool(t *testing.T) {
	pOpts := pool.NewObjectPoolOptions().SetSize(1)
	p := NewEncoderPool(pOpts)
	p.Init(NewNullEncoder)

	encoder := p.Get()
	assert.NotNil(t, encoder)

	p.Put(encoder)
}
