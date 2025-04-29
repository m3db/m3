package namespace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaticInit_Success(t *testing.T) {
	// Setup
	mds := MustBuildMetadatas(true, "ns1", "ns2")

	// Create initializer
	init := NewStaticInitializer(mds)
	assert.NotNil(t, init)

	// Initialize registry
	reg, err := init.Init()
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	// Watch should succeed
	watch, err := reg.Watch()
	assert.NoError(t, err)
	assert.NotNil(t, watch)

	// Close should not panic or error
	err = reg.Close()
	assert.NoError(t, err)
}
