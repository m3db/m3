package namespace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaticInit_Success(t *testing.T) {
	mds := MustBuildMetadatas(true, "ns1", "ns2")

	init := NewStaticInitializer(mds)
	assert.NotNil(t, init)

	reg, err := init.Init()
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	watch, err := reg.Watch()
	assert.NoError(t, err)
	assert.NotNil(t, watch)

	err = reg.Close()
	assert.NoError(t, err)
}
