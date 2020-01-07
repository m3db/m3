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

package instrument

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	testReportInterval = 10 * time.Millisecond
)

func newTestOptions() Options {
	testScope := tally.NewTestScope("", nil)
	return NewOptions().
		SetMetricsScope(testScope).
		SetReportInterval(testReportInterval)
}

func TestStartStop(t *testing.T) {
	defer leaktest.Check(t)()

	rep := NewBuildReporter(newTestOptions())
	require.NoError(t, rep.Start())
	require.NoError(t, rep.Stop())
}

func TestStartStart(t *testing.T) {
	defer leaktest.Check(t)()

	rep := NewBuildReporter(newTestOptions())
	defer rep.Stop()
	require.NoError(t, rep.Start())
	require.Error(t, rep.Start())
}

func TestStopWithoutStart(t *testing.T) {
	defer leaktest.Check(t)()

	rep := NewBuildReporter(newTestOptions())
	require.Error(t, rep.Stop())
}

func TestMultipleStop(t *testing.T) {
	defer leaktest.Check(t)()

	rep := NewBuildReporter(newTestOptions())
	require.NoError(t, rep.Start())
	go rep.Stop()
	go rep.Stop()
}

func TestVersionReported(t *testing.T) {
	defer leaktest.Check(t)()

	opts := newTestOptions()
	rep := NewBuildReporter(opts)
	require.NoError(t, rep.Start())

	testScope := opts.MetricsScope().(tally.TestScope)
	notFound := true
	for notFound {
		snapshot := testScope.Snapshot().Gauges()
		for key := range snapshot {
			if strings.Contains(key, buildInfoMetricName) {
				notFound = false
				break
			}
		}
	}

	require.NoError(t, rep.Stop())
}

func TestAgeReported(t *testing.T) {
	defer leaktest.Check(t)()

	opts := newTestOptions()
	rep := NewBuildReporter(opts)
	require.NoError(t, rep.Start())

	testScope := opts.MetricsScope().(tally.TestScope)
	notFound := true
	for notFound {
		snapshot := testScope.Snapshot().Gauges()
		for key := range snapshot {
			if strings.Contains(key, buildAgeMetricName) {
				notFound = false
				break
			}
		}
	}

	require.NoError(t, rep.Stop())
}

func TestAgeReportedIsCorrect(t *testing.T) {
	defer leaktest.Check(t)()

	BuildTimeUnix = fmt.Sprintf("%d", time.Now().Add(-24*time.Hour).Unix())
	opts := newTestOptions()
	rep := NewBuildReporter(opts)
	require.NoError(t, rep.Start())

	testScope := opts.MetricsScope().(tally.TestScope)
	notFound := true
	age := float64(0.0)
	for notFound {
		snapshot := testScope.Snapshot().Gauges()
		for key, value := range snapshot {
			if strings.Contains(key, buildAgeMetricName) {
				age = value.Value()
				notFound = false
				break
			}
		}
	}

	dayInNanos := float64(24 * time.Hour / time.Nanosecond)
	require.True(t, age >= dayInNanos)

	require.NoError(t, rep.Stop())
}

func TestBuildReporterFailsForWrongAge(t *testing.T) {
	defer leaktest.Check(t)()

	BuildTimeUnix = fmt.Sprintf("-%d", time.Now().Unix())
	opts := newTestOptions()
	rep := NewBuildReporter(opts)
	require.Error(t, rep.Start())
}
