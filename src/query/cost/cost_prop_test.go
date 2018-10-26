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
	"math"
	"sync"
	"testing"

	"github.com/m3db/m3/src/x/cost"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func TestPropertyPerQueryEnforcerAlwaysEndsUpZero(t *testing.T) {
	testParams := gopter.DefaultTestParameters()
	testParams.MinSuccessfulTests = 1000
	props := gopter.NewProperties(testParams)

	globalEndsUpZero := func(costs []float64, perQueryThreshold, globalThreshold float64) bool {
		pqfIFace, err := NewChainedEnforcer(
			"",
			[]cost.Enforcer{newTestEnforcer(cost.Limit{Threshold: cost.Cost(globalThreshold), Enabled: true}),
				newTestEnforcer(cost.Limit{Threshold: cost.Cost(perQueryThreshold), Enabled: true})})
		require.NoError(t, err)
		pqf := pqfIFace.(*chainedEnforcer)
		wg := sync.WaitGroup{}
		for _, c := range costs {
			wg.Add(1)
			go func(c float64) {
				defer wg.Done()

				perQuery := pqf.Child("query")
				defer perQuery.Release()
				perQuery.Add(cost.Cost(c))
			}(c)
		}

		wg.Wait()
		r, _ := pqf.local.State()

		// do delta comparison to deal with floating point errors. TODO: cost could potentially be an int
		const tolerance = 0.000001
		return math.Abs(float64(r.Cost)) < tolerance
	}

	props.Property("global enforcer >= 0",
		prop.ForAll(
			globalEndsUpZero,
			gen.SliceOf(gen.Float64Range(0.0, 10000.0)),
			gen.Float64Range(0.0, 10000.0),
			gen.Float64Range(0.0, 10000.0),
		))

	props.TestingRun(t)
}
