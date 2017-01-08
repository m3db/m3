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

package aggregator

import (
	"testing"

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestCounterElemPool(t *testing.T) {
	p := NewCounterElemPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init(func() *CounterElem {
		return NewCounterElem(nil, policy.Policy{}, testOptions())
	})

	// Retrieve an element from the pool
	element := p.Get()
	element.ResetSetData(testID, testPolicy)
	require.Equal(t, testID, element.id)
	require.Equal(t, testPolicy, element.policy)

	// Put the element back to pool
	p.Put(element)

	// Retrieve the element and assert it's the same element
	element = p.Get()
	require.Equal(t, testID, element.id)
	require.Equal(t, testPolicy, element.policy)
}

func TestTimerElemPool(t *testing.T) {
	p := NewTimerElemPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init(func() *TimerElem {
		return NewTimerElem(nil, policy.Policy{}, testOptions())
	})

	// Retrieve an element from the pool
	element := p.Get()
	element.ResetSetData(testID, testPolicy)
	require.Equal(t, testID, element.id)
	require.Equal(t, testPolicy, element.policy)

	// Put the element back to pool
	p.Put(element)

	// Retrieve the element and assert it's the same element
	element = p.Get()
	require.Equal(t, testID, element.id)
	require.Equal(t, testPolicy, element.policy)
}

func TestGaugeElemPool(t *testing.T) {
	p := NewGaugeElemPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init(func() *GaugeElem {
		return NewGaugeElem(nil, policy.Policy{}, testOptions())
	})

	// Retrieve an element from the pool
	element := p.Get()
	element.ResetSetData(testID, testPolicy)
	require.Equal(t, testID, element.id)
	require.Equal(t, testPolicy, element.policy)

	// Put the element back to pool
	p.Put(element)

	// Retrieve the element and assert it's the same element
	element = p.Get()
	require.Equal(t, testID, element.id)
	require.Equal(t, testPolicy, element.policy)
}
