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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3x/log"

	"github.com/stretchr/testify/assert"
)

func TestSource(t *testing.T) {
	testSource(t, 30, 25, 20)
	testSource(t, 22, 18, 20)
	testSource(t, 15, 10, 20)
	testSource(t, 28, 30, 20)
	testSource(t, 19, 21, 20)
	testSource(t, 13, 15, 20)
	testSource(t, 18, 22, 10)
	testSource(t, 14, 12, 15)
}

func testSource(t *testing.T, errAfter int32, closeAfter int32, watchNum int) {
	input := &testSourceInput{callCount: 0, errAfter: errAfter, closeAfter: closeAfter}
	s := NewSource(input, log.SimpleLogger)

	var wg sync.WaitGroup

	// Create a few watches.
	for i := 0; i < watchNum; i++ {
		wg.Add(1)
		_, w, err := s.Watch()
		assert.NoError(t, err)
		assert.NotNil(t, w)

		i := i
		go func() {
			var v interface{}
			count := 0
			for range w.C() {
				if v != nil {
					assert.True(t, w.Get().(int32) >= v.(int32))
				}
				v = w.Get()
				if count > i {
					w.Close()
				}
				count++
			}
			wg.Done()
		}()
	}

	// Only start serving after all watchers are created.
	input.startServing()

	// Schedule a thread to close Source.
	wg.Add(1)
	go func() {
		for !s.(*source).isClosed() {
			time.Sleep(time.Millisecond)
		}
		_, _, err := s.Watch()
		assert.Error(t, err)
		v := s.Get()
		if errAfter < closeAfter {
			assert.Equal(t, v, errAfter)
		} else {
			assert.Equal(t, v, closeAfter)
		}
		// Test Close again.
		s.Close()
		assert.True(t, s.(*source).isClosed())
		assert.Equal(t, v, s.Get())
		wg.Done()
	}()

	wg.Wait()
}

type testSourceInput struct {
	sync.Mutex

	started    bool
	callCount  int32
	errAfter   int32
	closeAfter int32
}

func (i *testSourceInput) startServing() {
	i.Lock()
	i.started = true
	i.Unlock()
}

func (i *testSourceInput) Poll() (interface{}, error) {
	i.Lock()
	started := i.started
	i.Unlock()
	if !started {
		return i.callCount, nil
	}
	if i.callCount >= i.closeAfter {
		return nil, ErrSourceClosed
	}
	i.callCount++
	time.Sleep(time.Millisecond)
	if i.errAfter > 0 {
		i.errAfter--
		return i.callCount, nil
	}
	return nil, errors.New("mock error")
}
