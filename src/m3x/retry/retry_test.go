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

package xretry

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	errTestFn = RetryableError(errors.New("an error"))
)

type testFnOpts struct {
	succeedAfter *int
	errs         []error
}

func newTestFn(opts testFnOpts) Fn {
	return func() error {
		if opts.succeedAfter != nil {
			if *opts.succeedAfter == 0 {
				return nil
			}
			*opts.succeedAfter--
		}
		if len(opts.errs) > 0 {
			err := opts.errs[0]
			opts.errs = opts.errs[1:]
			return err
		}
		return errTestFn
	}
}

func testOptions() Options {
	return NewOptions().
		SetInitialBackoff(time.Second).
		SetBackoffFactor(2).
		SetMaxRetries(2).
		SetJitter(false)
}

func TestRetrierExponentialBackOffSuccess(t *testing.T) {
	succeedAfter := 0
	slept := time.Duration(0)
	r := NewRetrier(testOptions()).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	err := r.Attempt(newTestFn(testFnOpts{succeedAfter: &succeedAfter}))
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(0), slept)
}

func TestRetrierExponentialBackOffSomeFailure(t *testing.T) {
	succeedAfter := 2
	slept := time.Duration(0)
	r := NewRetrier(testOptions()).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	err := r.Attempt(newTestFn(testFnOpts{succeedAfter: &succeedAfter}))
	assert.Nil(t, err)
	assert.Equal(t, 3*time.Second, slept)
}

func TestRetrierExponentialBackOffFailure(t *testing.T) {
	slept := time.Duration(0)
	r := NewRetrier(testOptions()).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	err := r.Attempt(newTestFn(testFnOpts{}))
	assert.Equal(t, errTestFn, err)
	assert.Equal(t, 3*time.Second, slept)
}

func TestRetrierMaxBackoff(t *testing.T) {
	succeedAfter := 3
	opts := testOptions().
		SetMaxRetries(succeedAfter).
		SetMaxBackoff(3 * time.Second)
	slept := time.Duration(0)
	r := NewRetrier(opts).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	err := r.Attempt(newTestFn(testFnOpts{succeedAfter: &succeedAfter}))
	assert.Nil(t, err)
	assert.Equal(t, 6*time.Second, slept)
}

func TestRetrierExponentialBackOffBreakWhileImmediate(t *testing.T) {
	slept := time.Duration(0)
	r := NewRetrier(testOptions()).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	err := r.AttemptWhile(func(_ int) bool { return false }, newTestFn(testFnOpts{}))
	assert.Equal(t, ErrWhileConditionFalse, err)
	assert.Equal(t, time.Duration(0), slept)
}

func TestRetrierExponentialBackOffBreakWhileSecondAttempt(t *testing.T) {
	slept := time.Duration(0)
	r := NewRetrier(testOptions()).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	err := r.AttemptWhile(func(attempt int) bool { return attempt == 0 }, newTestFn(testFnOpts{}))
	assert.Equal(t, ErrWhileConditionFalse, err)
	assert.Equal(t, time.Second, slept)
}

func TestRetrierExponentialBackOffJitter(t *testing.T) {
	succeedAfter := 1
	slept := time.Duration(0)
	r := NewRetrier(testOptions().SetJitter(true)).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	err := r.Attempt(newTestFn(testFnOpts{succeedAfter: &succeedAfter}))
	assert.Nil(t, err)
	// Test slept < time.Second as rand.Float64 range is [0.0, 1.0) and
	// also proves jitter is definitely applied
	assert.True(t, 500*time.Millisecond <= slept && slept < time.Second)
}

func TestRetrierExponentialBackOffNonRetryableErrorImmediate(t *testing.T) {
	slept := time.Duration(0)
	r := NewRetrier(testOptions()).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	expectedErr := NonRetryableError(fmt.Errorf("an error"))
	err := r.Attempt(newTestFn(testFnOpts{errs: []error{expectedErr}}))
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, time.Duration(0), slept)
}

func TestRetrierExponentialBackOffNonRetryableErrorSecondAttempt(t *testing.T) {
	slept := time.Duration(0)
	r := NewRetrier(testOptions()).(*retrier)
	r.sleepFn = func(t time.Duration) {
		slept += t
	}
	expectedErr := NonRetryableError(fmt.Errorf("an error"))
	err := r.Attempt(newTestFn(testFnOpts{errs: []error{errTestFn, expectedErr}}))
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, time.Second, slept)
}
