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

package util

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRefCount(t *testing.T) {
	rc := NewRefCount()
	assert.Equal(t, 0, rc.NumRef())

	var numSteps = 100
	for i := 0; i < numSteps; i++ {
		rc.IncRef()
		assert.Equal(t, i+1, rc.NumRef())
	}

	for i := 0; i < numSteps; i++ {
		rc.DecRef()
		assert.Equal(t, numSteps-i-1, rc.NumRef())
	}
}

func TestRefCountConcurrentUpdates(t *testing.T) {
	rc := NewRefCount()

	var (
		wg      sync.WaitGroup
		numIncs = 100
		numDecs = 50
	)
	wg.Add(numIncs + numDecs)

	// Ensure that the reference count is high enough so that the decrements will
	// never cause it to go negative.
	for i := 0; i < numDecs; i++ {
		rc.IncRef()
	}

	for i := 0; i < numIncs; i++ {
		go func() {
			rc.IncRef()
			wg.Done()
		}()
	}

	for i := 0; i < numDecs; i++ {
		go func() {
			rc.DecRef()
			wg.Done()
		}()
	}

	wg.Wait()

	assert.Equal(t, numDecs+(numIncs-numDecs), rc.NumRef())
}

func TestRefCountPanic(t *testing.T) {
	rc := NewRefCount()

	assert.Panics(t, func() {
		rc.DecRef()
	})
}
