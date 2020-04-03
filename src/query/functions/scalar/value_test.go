// Copyright (c) 2020 Uber Technologies, Inc.
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

package scalar

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/stretchr/testify/assert"
)

func TestApplyFunc(t *testing.T) {
	scalarThree := Value{Scalar: 3}
	scalarFive := Value{Scalar: 5}

	timedIdentity := Value{
		HasTimeValues: true,
		TimeValueFn: func(generator TimeValueGenerator) []float64 {
			return generator()
		},
	}

	timedNegative := Value{
		HasTimeValues: true,
		TimeValueFn: func(generator TimeValueGenerator) []float64 {
			genned := generator()
			for i, v := range genned {
				genned[i] = v * -1
			}

			return genned
		},
	}

	sum := func(x, y float64) float64 { return x + y }

	ac := ApplyFunc(scalarThree, scalarFive, sum)
	assert.False(t, ac.HasTimeValues)
	assert.Equal(t, float64(8), ac.Scalar)

	ac = ApplyFunc(ac, scalarThree, sum)
	assert.False(t, ac.HasTimeValues)
	assert.Equal(t, float64(11), ac.Scalar)

	gen := func() []float64 { return []float64{1, 2} }
	sub := func(x, y float64) float64 { return x - y }

	ac = ApplyFunc(timedIdentity, scalarFive, sub)
	assert.True(t, ac.HasTimeValues)
	assert.Equal(t, []float64{-4, -3}, ac.TimeValueFn(gen))

	ac = ApplyFunc(scalarThree, timedNegative, sum)
	assert.True(t, ac.HasTimeValues)
	assert.Equal(t, []float64{2, 1}, ac.TimeValueFn(gen))

	ac = ApplyFunc(ac, ac, sum)
	assert.True(t, ac.HasTimeValues)
	assert.Equal(t, []float64{4, 2}, ac.TimeValueFn(gen))

	ac = ApplyFunc(ac, timedIdentity, sum)
	assert.True(t, ac.HasTimeValues)
	assert.Equal(t, []float64{5, 4}, ac.TimeValueFn(gen))

	ac = ApplyFunc(ac, ac, sub)
	assert.True(t, ac.HasTimeValues)
	assert.Equal(t, []float64{0, 0}, ac.TimeValueFn(gen))
}

func TestTimeValueFromMetadata(t *testing.T) {
	startSecs := int64(100)
	start := time.Unix(startSecs, 0)
	generator := TimeValueFromMetadata(block.Metadata{
		Bounds: models.Bounds{
			Start:    start,
			Duration: 3 * time.Second,
			StepSize: time.Second,
		},
	})

	generated := generator()
	assert.Equal(t, []float64{100, 101, 102}, generated)
	generated[0] = 200
	// assure generated value is not affected by
	// mutations to previously generated values.
	regenerated := generator()
	assert.Equal(t, []float64{200, 101, 102}, generated)
	assert.Equal(t, []float64{100, 101, 102}, regenerated)
}
