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
	"time"

	"github.com/m3db/m3x/errors"
)

// RetriableError returns a retriable error
func RetriableError(err error) error {
	return xerrors.NewRetriableError(err)
}

// NonRetriableError returns a non-retriable error
func NonRetriableError(err error) error {
	return xerrors.NewNonRetriableError(err)
}

// Fn is a function that can be retried
type Fn func() error

// ContinueFn is a function that returns whether to continue attempting an operation
type ContinueFn func(attempt int) bool

// Retrier is a executor that can retry attempts on executing methods
type Retrier interface {
	// Attempt will attempt to perform a function with retries
	Attempt(fn Fn) error

	// Attempt will attempt to perform a function with retries
	AttemptWhile(continueFn ContinueFn, fn Fn) error
}

// Options is a set of retry options
type Options interface {
	// InitialBackoff sets the initial delay duration
	InitialBackoff(value time.Duration) Options

	// GetInitialBackoff gets the initial delay duration
	GetInitialBackoff() time.Duration

	// BackoffFactor sets the backoff factor multiplier when moving to next attempt
	BackoffFactor(value float64) Options

	// GetBackoffFactor gets the backoff factor multiplier when moving to next attempt
	GetBackoffFactor() float64

	// Max sets the maximum retry attempts
	Max(value int) Options

	// GetMax gets the maximum retry attempts
	GetMax() int

	// Jitter sets whether to jitter between the current backoff and the next
	// backoff when moving to next attempt
	Jitter(value bool) Options

	// GetJitter gets whether to jitter between the current backoff and the next
	// backoff when moving to next attempt
	GetJitter() bool
}
