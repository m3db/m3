// Copyright (c) 2021  Uber Technologies, Inc.
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

package resources

import (
	"time"

	"github.com/cenkalti/backoff/v3"
	"go.uber.org/zap"
)

const (
	retryMaxInterval = 5 * time.Second
	retryMaxTime     = 1 * time.Minute
)

// Retry is a function for retrying an operation in integration tests.
// Exponentially backs off between retries with a max wait of 5 seconds
// Waits up to a minute total for an operation to complete.
func Retry(op func() error) error {
	return RetryWithMaxTime(op, retryMaxTime)
}

// RetryWithMaxTime is a function for retrying an operation in integration tests.
// Exponentially backs off between retries with a max wait of 5 seconds
// Waits up to the max time provided to complete.
func RetryWithMaxTime(op func() error, maxTime time.Duration) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = retryMaxInterval
	bo.MaxElapsedTime = maxTime
	return backoff.Retry(op, bo)
}

// NewLogger creates a new development zap logger without stacktraces
// to cut down on verbosity.
func NewLogger() (*zap.Logger, error) {
	logCfg := zap.NewDevelopmentConfig()
	logCfg.DisableStacktrace = true

	return logCfg.Build()
}
