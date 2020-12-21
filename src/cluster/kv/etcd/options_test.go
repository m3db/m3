package etcd

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	opts := NewOptions()
	assert.NoError(t, opts.Validate())
	assert.Equal(t, defaultRequestTimeout, opts.RequestTimeout())
	assert.Equal(t, defaultWatchChanCheckInterval, opts.WatchChanCheckInterval())
	assert.Equal(t, defaultWatchChanResetInterval, opts.WatchChanCheckInterval())
	assert.Equal(t, defaultWatchChanInitTimeout, opts.WatchChanInitTimeout())
	assert.False(t, opts.EnableFastGets())
	ropts := opts.RetryOptions()
	assert.Equal(t, true, ropts.Jitter())
	assert.Equal(t, time.Second, ropts.InitialBackoff())
	assert.EqualValues(t, 2, ropts.BackoffFactor())
	assert.EqualValues(t, 5, ropts.MaxRetries())
	assert.Equal(t, time.Duration(math.MaxInt64), ropts.MaxBackoff())
}
