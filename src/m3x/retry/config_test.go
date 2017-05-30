// Copyright (c) 2017 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestRetryConfig(t *testing.T) {
	b1 := true
	b2 := false
	cfg := Configuration{
		InitialBackoff: time.Second,
		BackoffFactor:  2.0,
		MaxBackoff:     time.Minute,
		MaxRetries:     3,
		Forever:        &b1,
		Jitter:         &b2,
	}
	retrier := cfg.NewRetrier(tally.NoopScope).(*retrier)
	require.Equal(t, time.Second, retrier.initialBackoff)
	require.Equal(t, 2.0, retrier.backoffFactor)
	require.Equal(t, time.Minute, retrier.maxBackoff)
	require.Equal(t, 3, retrier.maxRetries)
	require.Equal(t, b1, retrier.forever)
	require.Equal(t, b2, retrier.jitter)
}
