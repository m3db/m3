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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testThresholdKey = "threshold"
	testEnabledKey   = "enabled"
)

func TestEnforcer(t *testing.T) {
	tests := []struct {
		input             Cost
		expected          Cost
		exceededThreshold bool
	}{
		{
			input:             Cost(0),
			expected:          0,
			exceededThreshold: false,
		},
		{
			input:             Cost(1),
			expected:          1,
			exceededThreshold: false,
		},
		{
			input:             Cost(3),
			expected:          4,
			exceededThreshold: false,
		},
		{
			input:             Cost(9),
			expected:          13,
			exceededThreshold: true,
		},
	}

	var (
		limit = Limit{
			Threshold: 10,
			Enabled:   true,
		}
		mOpts = NewLimitManagerOptions().SetDefaultLimit(limit)
		store = mem.NewStore()
		msg   = "message which contains context on the cost limit"
	)

	m, err := NewDynamicLimitManager(store, testThresholdKey, testEnabledKey, mOpts)
	require.NoError(t, err)

	opts := NewEnforcerOptions().SetCostExceededMessage(msg)
	e := NewEnforcer(m, NewTracker(), opts)

	for _, test := range tests {
		t.Run(fmt.Sprintf("input %v", test.input), func(t *testing.T) {
			report := e.Add(test.input)
			require.Equal(t, test.expected, report.Cost)

			if test.exceededThreshold {
				require.EqualError(t, report.Error, costExceededError(msg, 13, Limit{Threshold: 10}).Error())
			} else {
				require.NoError(t, report.Error)
			}
		})
	}

	// State should return the updated cost total.
	report, limit := e.State()
	require.Equal(t, Cost(13), report.Cost)
	require.Equal(t, Cost(10), limit.Threshold)
	require.True(t, limit.Enabled)
	require.EqualError(t, report.Error, costExceededError(msg, 13, Limit{Threshold: 10}).Error())

	// The error message should end with the message provided in the options.
	require.True(t, strings.HasSuffix(report.Error.Error(), msg))

	// When the threshold is raised, any new operations that stay below it should be legal again.
	store.Set(testThresholdKey, &commonpb.Float64Proto{Value: float64(20)})
	for {
		if l := e.Limit(); l.Threshold == 20 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	report = e.Add(Cost(3))
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, Cost(16), report.Cost)

	report = e.Add(Cost(5))
	require.NoError(t, err)
	require.NoError(t, err)
	require.EqualError(t, report.Error, costExceededError(msg, 21, Limit{Threshold: 20}).Error())
	require.Equal(t, Cost(21), report.Cost)

	// When the enforcer is disabled any input above the threshold should become legal.
	store.Set(testEnabledKey, &commonpb.BoolProto{Value: false})
	for {
		if l := e.Limit(); !l.Enabled {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	report = e.Add(Cost(2))
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, Cost(23), report.Cost)

	// State should return the updated state.
	report, limit = e.State()
	require.Equal(t, Cost(23), report.Cost)
	require.Equal(t, Cost(20), limit.Threshold)
	require.False(t, limit.Enabled)
	require.NoError(t, report.Error)
}

func TestEnforcerClone(t *testing.T) {
	var (
		store     = mem.NewStore()
		threshold = Cost(30)
		limit     = Limit{
			Threshold: threshold,
			Enabled:   true,
		}
		mOpts = NewLimitManagerOptions().
			SetDefaultLimit(limit)
	)

	m, err := NewDynamicLimitManager(store, testThresholdKey, testEnabledKey, mOpts)
	require.NoError(t, err)

	e := NewEnforcer(m, NewTracker(), nil)

	report := e.Add(Cost(10))
	require.Equal(t, Cost(10), report.Cost)
	require.NoError(t, report.Error)

	clone := e.Clone()

	// The cloned enforcer should have no initial cost.
	report, limit = clone.State()
	require.Equal(t, Cost(0), report.Cost)
	require.NoError(t, report.Error)
	require.Equal(t, threshold, limit.Threshold)
	require.True(t, limit.Enabled)

	// Subsequent calls to Add on each enforcer should be independent.
	report = e.Add(Cost(10))
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, Cost(20), report.Cost)

	report = clone.Add(Cost(5))
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, Cost(5), report.Cost)

	// Each enforcer should see the same updates to their state.
	var newThreshold Cost = 40
	store.Set(testThresholdKey, &commonpb.Float64Proto{Value: float64(newThreshold)})
	store.Set(testEnabledKey, &commonpb.BoolProto{Value: false})

	for {
		if l := e.Limit(); !l.Enabled {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	limit = e.Limit()
	require.Equal(t, false, limit.Enabled)
	require.Equal(t, newThreshold, limit.Threshold)

	limit = clone.Limit()
	require.Equal(t, false, limit.Enabled)
	require.Equal(t, newThreshold, limit.Threshold)
}

func TestNoopEnforcer(t *testing.T) {
	tests := []struct {
		input Cost
	}{
		{
			input: Cost(0),
		},
		{
			input: Cost(10),
		},
	}

	e := NoopEnforcer()
	limit := e.Limit()
	assert.Equal(t, MaxCost, limit.Threshold)
	assert.False(t, limit.Enabled)

	for _, test := range tests {
		t.Run(fmt.Sprintf("input %v", test.input), func(t *testing.T) {
			report := e.Add(test.input)
			assert.Equal(t, Cost(0), report.Cost)
			assert.NoError(t, report.Error)
		})
	}
}
