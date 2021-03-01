// Copyright (c) 2016 Uber Technologies, Inc.
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

package placement

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
)

func TestDeploymentOptions(t *testing.T) {
	dopts := NewDeploymentOptions()
	assert.Equal(t, defaultMaxStepSize, dopts.MaxStepSize())
	dopts = dopts.SetMaxStepSize(5)
	assert.Equal(t, 5, dopts.MaxStepSize())
}

func TestPlacementOptions(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		o := NewOptions()
		assert.True(t, o.AllowPartialReplace())
		assert.False(t, o.AddAllCandidates())
		assert.True(t, o.IsSharded())
		assert.Equal(t, IncludeTransitionalShardStates, o.ShardStateMode())
		assert.False(t, o.Dryrun())
		assert.False(t, o.IsMirrored())
		assert.False(t, o.IsStaged())
		assert.False(t, o.Compress())
		assert.NotNil(t, o.InstrumentOptions())
		assert.Equal(t, int64(0), o.PlacementCutoverNanosFn()())
		assert.Equal(t, int64(0), o.ShardCutoffNanosFn()())
		assert.Equal(t, int64(0), o.ShardCutoffNanosFn()())
		assert.Nil(t, o.InstanceSelector())
	})

	t.Run("setters", func(t *testing.T) {
		o := NewOptions()
		o = o.SetAllowPartialReplace(false)
		assert.False(t, o.AllowPartialReplace())

		o = o.SetAddAllCandidates(true)
		assert.True(t, o.AddAllCandidates())

		o = o.SetIsSharded(false)
		assert.False(t, o.IsSharded())

		o = o.SetShardStateMode(StableShardStateOnly)
		assert.Equal(t, StableShardStateOnly, o.ShardStateMode())

		o = o.SetDryrun(true)
		assert.True(t, o.Dryrun())

		o = o.SetIsMirrored(true)
		assert.True(t, o.IsMirrored())

		o = o.SetIsStaged(true)
		assert.True(t, o.IsStaged())

		o = o.SetCompress(true)
		assert.True(t, o.Compress())

		iopts := instrument.NewOptions().
			SetTimerOptions(instrument.TimerOptions{StandardSampleRate: 0.5})
		o = o.SetInstrumentOptions(iopts)
		assert.Equal(t, iopts, o.InstrumentOptions())

		o = o.SetPlacementCutoverNanosFn(func() int64 {
			return int64(10)
		})
		assert.Equal(t, int64(10), o.PlacementCutoverNanosFn()())

		o = o.SetShardCutoverNanosFn(func() int64 {
			return int64(20)
		})
		assert.Equal(t, int64(20), o.ShardCutoverNanosFn()())

		o = o.SetShardCutoffNanosFn(func() int64 {
			return int64(30)
		})
		assert.Equal(t, int64(30), o.ShardCutoffNanosFn()())

		now := time.Unix(0, 0)
		o = o.SetNowFn(func() time.Time {
			return now
		})
		assert.Equal(t, now, o.NowFn()())

		o = o.SetIsShardCutoverFn(func(s shard.Shard) error {
			return nil
		})
		assert.Nil(t, o.IsShardCutoverFn()(nil))

		o = o.SetIsShardCutoffFn(func(s shard.Shard) error {
			return nil
		})
		assert.Nil(t, o.IsShardCutoffFn()(nil))

		o = o.SetInstanceSelector(NewMockInstanceSelector(nil))
		assert.NotNil(t, o.InstanceSelector())
	})
}
