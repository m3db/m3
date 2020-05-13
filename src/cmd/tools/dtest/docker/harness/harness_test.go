package harness

import (
	"testing"
 
	"github.com/stretchr/testify/assert"
)

func TestHarness(t *testing.T) {
	assert.NoError(t, setupColdWrites())
}