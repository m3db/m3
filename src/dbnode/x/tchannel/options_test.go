package xtchannel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDefaultChannelOptions(t *testing.T) {
	opts := NewDefaultChannelOptions()

	assert.NotNil(t, opts, "Channel options should not be nil")

	// Check logger is the noop logger
	_, isNoopLogger := opts.Logger.(noopLogger)
	assert.True(t, isNoopLogger, "Logger should be a noop logger")

	// Check idle times
	assert.Equal(t, defaultMaxIdleTime, opts.MaxIdleTime, "MaxIdleTime mismatch")
	assert.Equal(t, defaultIdleCheckInterval, opts.IdleCheckInterval, "IdleCheckInterval mismatch")

	// Check default connection options
	assert.Equal(t, defaultSendBufferSize, opts.DefaultConnectionOptions.SendBufferSize, "SendBufferSize mismatch")
}
