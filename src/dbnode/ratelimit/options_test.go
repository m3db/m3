package ratelimit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionsAssignment(t *testing.T) {
	var (
		limitCheckEvery = 64
		limitEnabled    = true
		limitMbps       = 128.0
		rOpts           = NewOptions()
	)
	rOpts = rOpts.SetLimitCheckEvery(limitCheckEvery).
		SetLimitEnabled(limitEnabled).SetLimitMbps(limitMbps)

	require.Equal(t, rOpts.LimitCheckEvery(), limitCheckEvery)
	require.Equal(t, rOpts.LimitEnabled(), limitEnabled)
	require.Equal(t, rOpts.LimitMbps(), limitMbps)
}
