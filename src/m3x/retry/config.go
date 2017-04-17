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

	"github.com/uber-go/tally"
)

// Configuration configures options for retry attempts.
type Configuration struct {
	// Initial retry backoff.
	InitialBackoff time.Duration `yaml:"initialBackoff"`

	// Backoff factor for exponential backoff.
	BackoffFactor float64 `yaml:"backoffFactor"`

	// Maximum backoff time.
	MaxBackoff time.Duration `yaml:"maxBackoff"`

	// Maximum number of retry attempts.
	MaxRetries int `yaml:"maxRetries"`

	// Whether jittering is applied during retries.
	Jitter bool `yaml:"jitter"`
}

// NewRetrier creates a new retrier based on the configuration.
func (c Configuration) NewRetrier(scope tally.Scope) Retrier {
	var (
		initialBackoff = defaultInitialBackoff
		backoffFactor  = defaultBackoffFactor
		maxBackoff     = defaultMaxBackoff
		maxRetries     = defaultMaxRetries
	)
	if c.InitialBackoff != 0 {
		initialBackoff = c.InitialBackoff
	}
	if c.BackoffFactor != 0.0 {
		backoffFactor = c.BackoffFactor
	}
	if c.MaxBackoff != 0 {
		maxBackoff = c.MaxBackoff
	}
	if c.MaxRetries != 0 {
		maxRetries = c.MaxRetries
	}
	retryOpts := NewOptions().
		SetMetricsScope(scope).
		SetInitialBackoff(initialBackoff).
		SetBackoffFactor(backoffFactor).
		SetMaxBackoff(maxBackoff).
		SetMaxRetries(maxRetries).
		SetJitter(c.Jitter)
	return NewRetrier(retryOpts)
}
