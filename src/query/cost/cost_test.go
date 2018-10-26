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
	"math"
	"testing"

	"github.com/m3db/m3/src/x/cost"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChainedEnforcer_Child(t *testing.T) {
	t.Run("creates independent local enforcers with shared global enforcer", func(t *testing.T) {
		globalEnforcer := newTestEnforcer(cost.Limit{Threshold: 10.0, Enabled: true})
		localEnforcer := newTestEnforcer(cost.Limit{Threshold: 5.0, Enabled: true})

		pef, err := NewChainedEnforcer("", []cost.Enforcer{globalEnforcer, localEnforcer})
		require.NoError(t, err)

		l1, l2 := pef.Child("foo"), pef.Child("foo")

		l1.Add(2)

		assertCurCost(t, 2.0, l1)
		assertCurCost(t, 0.0, l2)
		assertCurCost(t, 2.0, globalEnforcer)
	})

	t.Run("returns a noop enforcer once out of models", func(t *testing.T) {
		globalEnforcer := newTestEnforcer(cost.Limit{Threshold: 10.0, Enabled: true})
		pef, err := NewChainedEnforcer("", []cost.Enforcer{globalEnforcer})
		require.NoError(t, err)

		child := pef.Child("foo")
		assert.Equal(t, noopChainedEnforcer, child)
	})
}

func TestChainedEnforcer_Release(t *testing.T) {
	t.Run("removes local total from global", func(t *testing.T) {
		parentIface, err := NewChainedEnforcer(
			"",
			[]cost.Enforcer{newTestEnforcer(cost.Limit{Threshold: 10.0, Enabled: true}),
				newTestEnforcer(cost.Limit{Threshold: 5.0, Enabled: true})})
		require.NoError(t, err)
		parent := parentIface.(*chainedEnforcer)

		pqe1, pqe2 := parent.Child("query"), parent.Child("query")

		pqe1.Add(cost.Cost(5.0))
		pqe1.Add(cost.Cost(6.0))

		pqe2.Add(cost.Cost(7.0))

		pqe1.Release()

		assertCurCost(t, cost.Cost(7.0), parent.local)
		pqe2.Release()
		assertCurCost(t, cost.Cost(0.0), parent.local)
	})

	t.Run("calls into reporter on release", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		makeEnforcer := func(cr chainedReporter) cost.Enforcer {
			return cost.NewEnforcer(cost.NewStaticLimitManager(cost.NewLimitManagerOptions()), cost.NewTracker(),
				cost.NewEnforcerOptions().SetReporter(cr))
		}

		makeReporter := func() *MockchainedReporter {
			r := NewMockchainedReporter(ctrl)
			r.EXPECT().ReportCurrent(gomock.Any()).AnyTimes()
			r.EXPECT().ReportOverLimit(gomock.Any()).AnyTimes()
			r.EXPECT().ReportCost(gomock.Any()).AnyTimes()
			return r
		}

		globalReporter, localReporter := makeReporter(), makeReporter()

		ce, err := NewChainedEnforcer(
			"global",
			[]cost.Enforcer{makeEnforcer(globalReporter), makeEnforcer(localReporter)})

		child := ce.Child("foo")
		child.Add(1.0)

		require.NoError(t, err)

		globalReporter.EXPECT().OnChildRelease(floatMatcher(1.0))
		localReporter.EXPECT().OnRelease(floatMatcher(1.0))

		child.Release()
	})
}

// floatMatcher does a janky delta comparison between floats, since rounding error makes float equality treacherous
type floatMatcher float64

func (f floatMatcher) Matches(x interface{}) bool {
	other, ok := x.(cost.Cost)
	if !ok {
		return false
	}

	// janky delta comparison
	return math.Abs(float64(f)-float64(other)) < 0.00001
}

func (f floatMatcher) String() string {
	return fmt.Sprintf("%f", f)
}

func TestChainedEnforcer_Add(t *testing.T) {
	assertGlobalError := func(t *testing.T, err error) {
		if assert.Error(t, err) {
			assert.Regexp(t, "exceeded global limit", err.Error())
		}
	}

	assertLocalError := func(t *testing.T, err error) {
		if assert.Error(t, err) {
			assert.Regexp(t, "exceeded query limit", err.Error())
		}
	}

	t.Run("errors on global error", func(t *testing.T) {
		pqe := newTestChainedEnforcer(5.0, 100.0)
		r := pqe.Add(cost.Cost(6.0))
		assertGlobalError(t, r.Error)
	})

	t.Run("errors on local error", func(t *testing.T) {
		pqe := newTestChainedEnforcer(100.0, 5.0)
		r := pqe.Add(cost.Cost(6.0))
		assertLocalError(t, r.Error)
	})

	t.Run("adds to local in case of global error", func(t *testing.T) {
		pqe := newTestChainedEnforcer(5.0, 100.0)
		r := pqe.Add(cost.Cost(6.0))
		assertGlobalError(t, r.Error)

		r, _ = pqe.State()
		assert.Equal(t, cost.Report{
			Error: nil,
			Cost:  6.0},
			r)
	})

	t.Run("adds to global in case of local error", func(t *testing.T) {
		pqe := newTestChainedEnforcer(100.0, 5.0)
		r := pqe.Add(cost.Cost(6.0))
		assertLocalError(t, r.Error)

		r, _ = pqe.parent.State()
		assert.Equal(t, cost.Report{
			Error: nil,
			Cost:  6.0},
			r)
	})

	t.Run("release after local error", func(t *testing.T) {
		pqe := newTestChainedEnforcer(10.0, 5.0)

		// exceeds local
		r := pqe.Add(6.0)
		assertLocalError(t, r.Error)

		pqe.Release()
		assertCurCost(t, 0.0, pqe.local)
	})

	t.Run("release after global error", func(t *testing.T) {
		pqe := newTestChainedEnforcer(5.0, 10.0)
		// exceeds global
		r := pqe.Add(6.0)
		assertGlobalError(t, r.Error)
		pqe.Release()
		assertCurCost(t, 0.0, pqe.local)
	})
}

func TestChainedEnforcer_State(t *testing.T) {
	pqe := newTestChainedEnforcer(10.0, 5.0)
	pqe.Add(15.0)

	r, l := pqe.State()
	assert.Equal(t, cost.Cost(15.0), r.Cost)
	assert.EqualError(t, r.Error, "15 exceeds limit of 5")
	assert.Equal(t, cost.Limit{Threshold: 5.0, Enabled: true}, l)
}

func TestNoopChainedEnforcer_Release(t *testing.T) {
	ce := NoopChainedEnforcer()
	ce.Release()
	assertCurCost(t, 0.0, ce)
}

// utils

func newTestEnforcer(limit cost.Limit) cost.Enforcer {
	return cost.NewEnforcer(
		cost.NewStaticLimitManager(cost.NewLimitManagerOptions().SetDefaultLimit(limit)),
		cost.NewTracker(),
		nil,
	)
}

func newTestChainedEnforcer(globalLimit float64, localLimit float64) *chainedEnforcer {
	rtn, err := NewChainedEnforcer(
		"global",
		[]cost.Enforcer{newTestEnforcer(cost.Limit{Threshold: cost.Cost(globalLimit), Enabled: true}),
			newTestEnforcer(cost.Limit{Threshold: cost.Cost(localLimit), Enabled: true})})
	if err != nil {
		panic(err.Error())
	}

	return rtn.Child("query").(*chainedEnforcer)
}

func assertCurCost(t *testing.T, expectedCost cost.Cost, ef cost.Enforcer) {
	actual, _ := ef.State()
	assert.Equal(t, cost.Report{
		Cost:  expectedCost,
		Error: nil,
	}, actual)
}
