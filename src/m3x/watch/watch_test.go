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

package watch

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWatchable(t *testing.T) {
	p := NewWatchable()
	assert.Nil(t, p.Get())
	assert.Equal(t, 0, WatchLen(p.(*watchable)))
	assert.NoError(t, p.Update(nil))
	get := 100
	p = NewWatchable()
	p.Update(get)
	assert.Equal(t, get, p.Get())
	v, s, err := p.Watch()
	assert.NotNil(t, s)
	assert.Equal(t, get, v)
	assert.NoError(t, err)
	assert.NoError(t, p.Update(get))
	assert.Equal(t, 1, WatchLen(p.(*watchable)))

	p.Close()
	assert.Equal(t, 0, WatchLen(p.(*watchable)))
	assert.Equal(t, get, p.Get())
	_, s, err = p.Watch()
	assert.Nil(t, s)
	assert.Equal(t, errClosed, err)
	assert.Equal(t, errClosed, p.Update(get))
	assert.NotPanics(t, p.Close)
}

func TestWatch(t *testing.T) {
	p := NewWatchable()
	_, s, err := p.Watch()
	assert.NoError(t, err)

	err = p.Update(nil)
	assert.NoError(t, err)

	_, ok := <-s.C()
	assert.True(t, ok)
	assert.Nil(t, s.Get())

	assert.Equal(t, 1, WatchLen(p.(*watchable)))
	s.Close()
	_, ok = <-s.C()
	assert.False(t, ok)
	assert.Equal(t, 0, WatchLen(p.(*watchable)))
	assert.NotPanics(t, s.Close)

	get := 100
	p = NewWatchable()
	_, s, err = p.Watch()
	assert.NoError(t, err)

	err = p.Update(get)
	assert.Equal(t, get, p.Get())
	assert.NoError(t, err)
	_, ok = <-s.C()
	assert.True(t, ok)
	assert.Equal(t, get, s.Get())

	// sub.Close() after p.Close()
	assert.Equal(t, 1, WatchLen(p.(*watchable)))
	p.Close()
	assert.Equal(t, 0, WatchLen(p.(*watchable)))
	s.Close()
	_, ok = <-s.C()
	assert.False(t, ok)
	assert.Equal(t, 0, WatchLen(p.(*watchable)))
}

func TestMultiWatch(t *testing.T) {
	p := NewWatchable()
	subLen := 20
	subMap := make(map[int]Watch, subLen)
	valueMap := make(map[int]int, subLen)
	for i := 0; i < subLen; i++ {
		_, s, err := p.Watch()
		assert.NoError(t, err)
		subMap[i] = s
		valueMap[i] = -1
	}

	for i := 0; i < subLen; i++ {
		testWatchAndClose(t, p, subMap, valueMap, i)
	}

	assert.Equal(t, 0, WatchLen(p.(*watchable)))
	p.Close()
}

func testWatchAndClose(t *testing.T, p Watchable, subMap map[int]Watch, valueMap map[int]int, value interface{}) {
	err := p.Update(value)
	assert.NoError(t, err)

	for i, s := range subMap {
		_, ok := <-s.C()
		assert.True(t, ok)
		v := s.Get().(int)
		assert.True(t, v > valueMap[i], fmt.Sprintf("Get() value should be > than before: %v, %v", v, valueMap[i]))
		valueMap[i] = v
	}

	l := WatchLen(p.(*watchable))
	assert.Equal(t, len(subMap), l)

	// randomly close 1 subscriber
	for i, s := range subMap {
		s.Close()
		_, ok := <-s.C()
		assert.False(t, ok)
		p.Get()
		delete(subMap, i)
		delete(valueMap, i)
		break
	}
	assert.Equal(t, l-1, WatchLen(p.(*watchable)))
}

func TestAsyncWatch(t *testing.T) {
	p := NewWatchable()

	subLen := 10
	var wg sync.WaitGroup

	for i := 0; i < subLen; i++ {
		_, s, err := p.Watch()
		assert.NoError(t, err)

		wg.Add(1)
		go func() {
			for _ = range s.C() {
				r := rand.Int63n(100)
				time.Sleep(time.Millisecond * time.Duration(r))
			}
			_, ok := <-s.C()
			// chan got closed
			assert.False(t, ok)
			// got the latest value
			assert.Equal(t, subLen-1, s.Get())
			wg.Done()
		}()
	}

	for i := 0; i < subLen; i++ {
		err := p.Update(i)
		assert.NoError(t, err)
	}
	p.Close()
	assert.Equal(t, 0, WatchLen(p.(*watchable)))
	wg.Wait()
}

func WatchLen(w *watchable) int {
	w.RLock()
	l := len(w.active)
	w.RUnlock()
	return l
}
