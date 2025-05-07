// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/dbnode/ratelimit"
	"github.com/m3db/m3/src/dbnode/topology"
)

func TestRuntimeOptionsDefaultsIsValid(t *testing.T) {
	v := NewOptions()
	assert.NoError(t, v.Validate())
}

func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(o Options) Options
		wantErr error
	}{
		{
			name:    "Valid options",
			modify:  func(o Options) Options { return o },
			wantErr: nil,
		},
		{
			name: "Negative writeNewSeriesBackoffDuration",
			modify: func(o Options) Options {
				return o.SetWriteNewSeriesBackoffDuration(-1)
			},
			wantErr: errWriteNewSeriesBackoffDurationIsNegative,
		},
		{
			name: "Negative writeNewSeriesLimitPerShardPerSecond",
			modify: func(o Options) Options {
				return o.SetWriteNewSeriesLimitPerShardPerSecond(-1)
			},
			wantErr: errWriteNewSeriesLimitPerShardPerSecondIsNegative,
		},
		{
			name: "Zero tickSeriesBatchSize",
			modify: func(o Options) Options {
				return o.SetTickSeriesBatchSize(0)
			},
			wantErr: errTickSeriesBatchSizeMustBePositive,
		},
		{
			name: "Zero tickPerSeriesSleepDuration",
			modify: func(o Options) Options {
				return o.SetTickPerSeriesSleepDuration(0)
			},
			wantErr: errTickPerSeriesSleepDurationMustBePositive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.modify(NewOptions())
			err := opts.Validate()

			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestRuntimeOptions(t *testing.T) {
	opts := NewOptions()
	newRateLimitOpts := ratelimit.NewOptions().SetLimitMbps(100)
	opts = opts.SetPersistRateLimitOptions(newRateLimitOpts)
	assert.Equal(t, newRateLimitOpts, opts.PersistRateLimitOptions())

	opts = opts.SetWriteNewSeriesAsync(true)
	assert.True(t, opts.WriteNewSeriesAsync())

	opts = opts.SetWriteNewSeriesBackoffDuration(5 * time.Second)
	assert.Equal(t, 5*time.Second, opts.WriteNewSeriesBackoffDuration())

	opts = opts.SetWriteNewSeriesLimitPerShardPerSecond(10)
	assert.Equal(t, 10, opts.WriteNewSeriesLimitPerShardPerSecond())

	opts = opts.SetEncodersPerBlockLimit(50)
	assert.Equal(t, 50, opts.EncodersPerBlockLimit())

	opts = opts.SetTickSeriesBatchSize(100)
	assert.Equal(t, 100, opts.TickSeriesBatchSize())

	opts = opts.SetTickPerSeriesSleepDuration(500 * time.Millisecond)
	assert.Equal(t, 500*time.Millisecond, opts.TickPerSeriesSleepDuration())

	opts = opts.SetTickMinimumInterval(15 * time.Second)
	assert.Equal(t, 15*time.Second, opts.TickMinimumInterval())

	opts = opts.SetMaxWiredBlocks(50000)
	assert.Equal(t, uint(50000), opts.MaxWiredBlocks())

	opts = opts.SetClientBootstrapConsistencyLevel(topology.ReadConsistencyLevelAll)
	assert.Equal(t, topology.ReadConsistencyLevelAll, opts.ClientBootstrapConsistencyLevel())

	opts = opts.SetClientReadConsistencyLevel(topology.ReadConsistencyLevelOne)
	assert.Equal(t, topology.ReadConsistencyLevelOne, opts.ClientReadConsistencyLevel())

	opts = opts.SetClientWriteConsistencyLevel(topology.ConsistencyLevelAll)
	assert.Equal(t, topology.ConsistencyLevelAll, opts.ClientWriteConsistencyLevel())

	opts = opts.SetTickCancellationCheckInterval(30 * time.Second)
	assert.Equal(t, 30*time.Second, opts.TickCancellationCheckInterval())
}
