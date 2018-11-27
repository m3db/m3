// Copyright (c) 2018 Uber Technologies, Inc.
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
//

package cost

import (
	"errors"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamicLimitManager(t *testing.T) {
	var (
		threshold    Cost = 100
		enabled           = true
		store             = mem.NewStore()
		thresholdKey      = "threshold"
		enabledKey        = "enabled"
	)

	validateFn := func(data interface{}) error {
		val := data.(float64)
		if val < 50 {
			return errors.New("limit cannot be below 50")
		}
		return nil
	}

	var (
		limit = Limit{
			Threshold: threshold,
			Enabled:   enabled,
		}
		opts = NewLimitManagerOptions().
			SetDefaultLimit(limit).
			SetValidateLimitFn(validateFn)
	)

	m, err := NewDynamicLimitManager(
		store, thresholdKey, enabledKey, opts,
	)
	require.NoError(t, err)

	testLimitManager(t, m, threshold, enabled)

	// Test updates to the threshold.
	threshold = 200
	store.Set(thresholdKey, &commonpb.Float64Proto{Value: float64(threshold)})

	for {
		if m.Limit().Threshold == threshold {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	testLimitManager(t, m, threshold, enabled)

	// Test updates to enabled.
	enabled = false
	store.Set(enabledKey, &commonpb.BoolProto{Value: enabled})

	for {
		if !m.Limit().Enabled {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	testLimitManager(t, m, threshold, enabled)

	// Test that invalid updates are ignored.
	store.Set(thresholdKey, &commonpb.Float64Proto{Value: 25})
	time.Sleep(100 * time.Millisecond)

	testLimitManager(t, m, threshold, enabled)

	// Ensure we do not leak any goroutines.
	m.Close()
	leaktest.Check(t)
}

func TestStaticLimitManager(t *testing.T) {
	var (
		threshold = Cost(100)
		enabled   = true
		limit     = Limit{
			Threshold: threshold,
			Enabled:   enabled,
		}
		manager = NewStaticLimitManager(NewLimitManagerOptions().SetDefaultLimit(limit))
	)

	limit = manager.Limit()
	assert.Equal(t, threshold, limit.Threshold)
	assert.Equal(t, enabled, limit.Enabled)

	// Ensure we do not leak any goroutines.
	manager.Close()
	leaktest.Check(t)
}

func testLimitManager(
	t *testing.T, m LimitManager, expectedThreshold Cost, expectedEnabled bool,
) {
	l := m.Limit()
	assert.Equal(t, expectedThreshold, l.Threshold)
	assert.Equal(t, expectedEnabled, l.Enabled)
}
