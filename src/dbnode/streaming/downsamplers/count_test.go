package downsamplers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCountDownsampler(t *testing.T) {
	downsampler := NewCountDownsampler()

	downsampler.Accept(1.1)
	downsampler.Accept(2.2)
	downsampler.Accept(3.3)
	assert.Equal(t, 3.0, downsampler.Emit())

	downsampler.Accept(0)
	downsampler.Accept(-1)
	assert.Equal(t, 2.0, downsampler.Emit())

	downsampler.Accept(0)
	assert.Equal(t, 1.0, downsampler.Emit())
}
