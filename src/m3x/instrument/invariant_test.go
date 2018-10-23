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

package instrument_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/stretchr/testify/require"

	"github.com/uber-go/tally"
)

func ExampleInvariantViolatedMetricInvocation() {
	testScope := tally.NewTestScope("", nil)
	opts := instrument.NewOptions().SetMetricsScope(testScope)

	instrument.EmitInvariantViolation(opts)
	counters := testScope.Snapshot().Counters()
	counter := counters[instrument.InvariantViolatedMetricName+"+"]
	fmt.Println(counter.Name(), counter.Value())
	// Output: invariant_violated 1
}

func TestEmitInvariantViolationDoesNotPanicIfEnvNotSet(t *testing.T) {
	opts := instrument.NewOptions()
	require.NotPanics(t, func() {
		instrument.EmitInvariantViolation(opts)
	})
}

func TestEmitAndLogInvariantViolationDoesNotPanicIfEnvNotSet(t *testing.T) {
	opts := instrument.NewOptions()
	require.NotPanics(t, func() {
		instrument.EmitAndLogInvariantViolation(opts, func(l log.Logger) {
			l.Error("some_error")
		})
	})
}

func TestErrorfDoesNotPanicIfEnvNotSet(t *testing.T) {
	var (
		format      = "some error format: %s"
		err_msg     = "error message"
		expectedErr = fmt.Errorf("invariant_violated: some error format: error message")
	)
	require.NotPanics(t, func() {
		require.Equal(
			t, expectedErr, instrument.InvariantErrorf(format, err_msg))
	})
}

func TestEmitInvariantViolationPanicsIfEnvSet(t *testing.T) {
	defer setShouldPanicEnvironmentVariable()()

	opts := instrument.NewOptions()
	require.Panics(t, func() {
		instrument.EmitInvariantViolation(opts)
	})
}

func TestEmitAndLogInvariantViolationPanicsIfEnvSet(t *testing.T) {
	defer setShouldPanicEnvironmentVariable()()

	opts := instrument.NewOptions()
	require.Panics(t, func() {
		instrument.EmitAndLogInvariantViolation(opts, func(l log.Logger) {
			l.Error("some_error")
		})
	})
}

func TestErrorfPanicsIfEnvSet(t *testing.T) {
	defer setShouldPanicEnvironmentVariable()()
	require.Panics(t, func() {
		instrument.InvariantErrorf("some_error")
	})
}

type cleanupFn func()

func setShouldPanicEnvironmentVariable() cleanupFn {
	restoreValue := os.Getenv(instrument.ShouldPanicEnvironmentVariableName)
	os.Setenv(instrument.ShouldPanicEnvironmentVariableName, "true")
	return cleanupFn(func() {
		os.Setenv(instrument.ShouldPanicEnvironmentVariableName, restoreValue)
	})
}
