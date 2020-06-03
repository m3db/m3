// Copyright (c) 2019 Uber Technologies, Inc.
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

package resource

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestCancellableLifetime(t *testing.T) {
	l := NewCancellableLifetime()

	checkouts := 42

	for i := 0; i < checkouts; i++ {
		ok := l.TryCheckout()
		require.True(t, ok)
	}

	// Launch cancel goroutine
	cancelDone := atomic.NewBool(false)
	go func() {
		l.Cancel()
		cancelDone.Store(true)
	}()

	for i := 0; i < checkouts; i++ {
		time.Sleep(2 * time.Millisecond)

		// Ensure not done yet until final release
		done := cancelDone.Load()
		require.False(t, done)

		l.ReleaseCheckout()
	}

	// Ensure cannot checkout any longer
	ok := l.TryCheckout()
	require.False(t, ok)

	// Ensure that cancel finished
	for {
		if cancelDone.Load() {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
}
