package checked

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSetFinalized(t *testing.T) {
	obj := RefCount{}
	assert.False(t, obj.Finalized())

	obj.SetFinalized(true)
	assert.True(t, obj.Finalized())

	obj.SetFinalized(false)
	assert.False(t, obj.Finalized())
}
