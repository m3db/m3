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
	"sync/atomic"
)

func TestSource(t *testing.T) {
	testSource(t, 30, 25, 20)
	testSource(t, 22, 18, 20)
	testSource(t, 15, 10, 20)
	testSource(t, 28, 30, 20)
	testSource(t, 19, 21, 20)
	testSource(t, 13, 15, 20)
}

func testSource(t *testing.T, inputErrAfter int, closeAfter int32, watchNum int) {
	var callCount int32
	input := testPollFn(inputErrAfter, &callCount)
	s := NewSource(input, xlog.SimpleLogger)

	var wg sync.WaitGroup

	// create a few watches
	for i := 0; i < watchNum; i++ {
		wg.Add(1)
		_, o, err := s.Watch()
		assert.NoError(t, err)

		i := i
		go func() {
			var v interface{}
			count := 0
			for _ = range o.C() {
				if v != nil {
					assert.True(t, o.Get().(int64) >= v.(int64))
				}
				v = o.Get()
				if count > i {
					o.Close()
				}
				count++
			}
			wg.Done()
		}()
	}

	// schedule a thread to close Source
	wg.Add(1)
	go func() {
		for atomic.LoadInt32(&callCount) < closeAfter {
			time.Sleep(1 * time.Millisecond)
		}
		s.Close()
		assert.True(t, s.(*source).isClosed())
		// test Close again
		s.Close()
		assert.True(t, s.(*source).isClosed())
		wg.Done()
	}()

	wg.Wait()
}

func testPollFn(errAfter int, callCount *int32) (SourcePollFn) {
	return func() (interface{}, error) {
		atomic.AddInt32(callCount, 1)
		time.Sleep(time.Millisecond)
		if errAfter > 0 {
			errAfter--
			return time.Now().Unix(), nil
		}
		return nil, errors.New("mock error")
	}
}
