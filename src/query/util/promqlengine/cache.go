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
	"sync"
	"time"

	"github.com/prometheus/prometheus/promql"
)

// NewEngineFn is a function to create a new promql.Engine with given lookback duration.
type NewEngineFn func(lookbackDelta time.Duration) (*promql.Engine, error)

// Cache caches PromQL engines by lookback duration.
type Cache struct {
	sync.RWMutex

	cachedEngines map[time.Duration]*promql.Engine
	newEngineFn   NewEngineFn
}

// NewCache creates a new Cache.
func NewCache(newEngineFn NewEngineFn) *Cache {
	return &Cache{
		cachedEngines: make(map[time.Duration]*promql.Engine),
		newEngineFn:   newEngineFn,
	}
}

// Get returns a new or cached promql.Engine.
func (c *Cache) Get(lookbackDelta time.Duration) (*promql.Engine, error) {
	var (
		engine *promql.Engine
		ok     bool
		err    error
	)
	c.RLock()
	engine, ok = c.cachedEngines[lookbackDelta]
	c.RUnlock()
	if ok {
		return engine, nil
	}

	c.Lock()
	defer c.Unlock()
	if engine, ok = c.cachedEngines[lookbackDelta]; ok {
		return engine, nil
	}

	if engine, err = c.newEngineFn(lookbackDelta); err != nil {
		return nil, err
	}

	c.cachedEngines[lookbackDelta] = engine
	return engine, nil
}

// Set allows manually caching promql.Engine.
func (c *Cache) Set(lookbackDelta time.Duration, engine *promql.Engine) {
	c.Lock()
	defer c.Unlock()
	c.cachedEngines[lookbackDelta] = engine
}
