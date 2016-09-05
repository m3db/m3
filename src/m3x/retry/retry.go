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
	"math"
	"math/rand"
	"time"

	"github.com/m3db/m3x/errors"

	"github.com/uber-go/tally"
)

var (
	// ErrWhileConditionFalse is raised when the while condition to a while retry method evaluates false
	ErrWhileConditionFalse = errors.New("retry while condition evaluated to false")
)

type retrier struct {
	initialBackoff time.Duration
	backoffFactor  float64
	max            int
	jitter         bool
	sleepFn        func(t time.Duration)
	metrics        retrierMetrics
}

type retrierMetrics struct {
	success            tally.Counter
	successLatency     tally.Timer
	errors             tally.Counter
	errorsNotRetryable tally.Counter
	errorsFinal        tally.Counter
	errorsLatency      tally.Timer
	retries            tally.Counter
}

// NewRetrier creates a new retrier
func NewRetrier(opts Options) Retrier {
	scope := opts.GetMetricsScope()
	errorTags := struct {
		retryable    map[string]string
		notRetryable map[string]string
	}{
		map[string]string{
			"type": "retryable",
		},
		map[string]string{
			"type": "not-retryable",
		},
	}
	return &retrier{
		initialBackoff: opts.GetInitialBackoff(),
		backoffFactor:  opts.GetBackoffFactor(),
		max:            opts.GetMax(),
		jitter:         opts.GetJitter(),
		sleepFn:        time.Sleep,
		metrics: retrierMetrics{
			success:            scope.Counter("success"),
			successLatency:     scope.Timer("success-latency"),
			errors:             scope.Tagged(errorTags.retryable).Counter("errors"),
			errorsNotRetryable: scope.Tagged(errorTags.notRetryable).Counter("errors"),
			errorsFinal:        scope.Counter("errors-final"),
			errorsLatency:      scope.Timer("errors-latency"),
			retries:            scope.Counter("retries"),
		},
	}
}

func (r *retrier) Attempt(fn Fn) error {
	return r.attempt(nil, fn)
}

func (r *retrier) AttemptWhile(continueFn ContinueFn, fn Fn) error {
	return r.attempt(continueFn, fn)
}

func (r *retrier) attempt(continueFn ContinueFn, fn Fn) error {
	attempt := 0

	if continueFn != nil && !continueFn(attempt) {
		return ErrWhileConditionFalse
	}

	err := fn()
	attempt++
	if err == nil {
		r.metrics.success.Inc(1)
		return nil
	}
	if xerrors.IsNonRetryableError(err) {
		r.metrics.errorsNotRetryable.Inc(1)
		return err
	}
	r.metrics.errors.Inc(1)

	for i := 0; i < r.max; i++ {
		curr := r.initialBackoff.Nanoseconds() * int64(math.Pow(r.backoffFactor, float64(i)))
		if r.jitter {
			half := curr / 2
			curr = half + int64(rand.Float64()*float64(half))
		}
		r.sleepFn(time.Duration(curr))

		if continueFn != nil && !continueFn(attempt) {
			return ErrWhileConditionFalse
		}

		r.metrics.retries.Inc(1)
		err = fn()
		attempt++
		if err == nil {
			r.metrics.success.Inc(1)
			return nil
		}
		if xerrors.IsNonRetryableError(err) {
			r.metrics.errorsNotRetryable.Inc(1)
			return err
		}
		r.metrics.errors.Inc(1)
	}
	r.metrics.errorsFinal.Inc(1)

	return err
}
