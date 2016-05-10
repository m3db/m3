// Copyright (c) 2015 Uber Technologies, Inc.

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

package goroutines

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// At baseline, we expect to see one goroutine in the testing package's main
// function and one in syscall.goexit.
const _expectedRuntimeGoroutines = 2

// filterStacks will filter any stacks excluded by the given VerifyOpts.
func filterStacks(stacks []Stack, opts *VerifyOpts) []Stack {
	filtered := stacks[:0]
	for _, stack := range stacks {
		if opts.ShouldSkip(stack) {
			continue
		}
		filtered = append(filtered, stack)
	}
	return filtered
}

// IdentifyLeaks looks for extra goroutines, and returns a descriptive error if
// it finds any.
func IdentifyLeaks(opts *VerifyOpts) error {
	const maxAttempts = 50
	var stacks []Stack
	for i := 0; i < maxAttempts; i++ {
		// Ignore the first stack, which is the current goroutine (the one
		// that's doing the verification).
		stacks = GetAll()[1:]
		stacks = filterStacks(stacks, opts)

		if len(stacks) <= _expectedRuntimeGoroutines {
			return nil
		}

		if i > maxAttempts/2 {
			time.Sleep(time.Duration(i) * time.Millisecond)
		} else {
			runtime.Gosched()
		}
	}

	return fmt.Errorf("expected at most %v goroutines, found more:\n%s", _expectedRuntimeGoroutines, stacks)
}

// VerifyNoLeaks calls IdentifyLeaks and fails the test if it finds any leaked
// goroutines.
func VerifyNoLeaks(t testing.TB, opts *VerifyOpts) {
	if err := IdentifyLeaks(opts); err != nil {
		t.Error(err.Error())
	}
}
