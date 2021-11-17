// Copyright (c) 2021 Uber Technologies, Inc.
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

package promqlengine

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	lookback := time.Second
	expected := newTestEngine(lookback)
	cache := NewCache(map[time.Duration]*promql.Engine{}, func(l time.Duration) (*promql.Engine, error) {
		if l == lookback {
			return expected, nil
		}
		return &promql.Engine{}, errors.New(l.String())
	})

	actual, err := cache.Get(lookback)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	_, err = cache.Get(time.Minute)
	require.EqualError(t, err, time.Minute.String())
}

func TestDefaultEngines(t *testing.T) {
	expecteds := map[time.Duration]*promql.Engine{
		time.Second: newTestEngine(time.Second),
		time.Minute: newTestEngine(time.Minute),
		time.Hour:   newTestEngine(time.Hour),
	}

	cache := NewCache(expecteds, func(l time.Duration) (*promql.Engine, error) {
		return &promql.Engine{}, errors.New("not set")
	})

	for l, expected := range expecteds {
		engine, err := cache.Get(l)
		require.NoError(t, err)
		require.Equal(t, expected, engine)
	}
}

func newTestEngine(l time.Duration) *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		LookbackDelta: l,
	})
}
