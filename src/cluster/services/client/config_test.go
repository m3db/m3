package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	cf := Configuration{}
	require.Equal(t, defaultInitTimeout, cf.NewOptions().InitTimeout())

	cf = Configuration{InitTimeout: time.Second}
	require.Equal(t, time.Second, cf.NewOptions().InitTimeout())
}
