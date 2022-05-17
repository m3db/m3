// Copyright (c) 2021 Uber Technologies, Inc.
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

package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestBootstrapRetryMetricReason(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedReason string
	}{
		{
			name:           "any error",
			err:            xerrors.NewInvalidParamsError(errors.New("some error")),
			expectedReason: "other",
		},
		{
			name:           "obsolete ranges error",
			err:            xerrors.NewInvalidParamsError(bootstrap.ErrFileSetSnapshotTypeRangeAdvanced),
			expectedReason: "obsolete-ranges",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testScope := tally.NewTestScope("testScope", map[string]string{})
			options := NewOptions()
			options = options.SetInstrumentOptions(options.InstrumentOptions().SetMetricsScope(testScope))
			instr := newBootstrapInstrumentation(options)

			instr.bootstrapFailed(1, tt.err)

			retryMetrics := getBootstrapRetryMetrics(testScope)
			assert.NotEmpty(t, retryMetrics)
			assert.Equal(t, 1, retryMetrics[tt.expectedReason], "metric with reason:%v should be set to 1")
			for reason, value := range retryMetrics {
				if reason != tt.expectedReason {
					assert.Equal(t, 0, value, "metric with reason:%v should stay 0")
				}
			}
		})
	}
}

func getBootstrapRetryMetrics(testScope tally.TestScope) map[string]int {
	const (
		metricName = "bootstrap-retries"
		reasonTag  = "reason"
	)
	valuesByReason := make(map[string]int)
	for _, counter := range testScope.Snapshot().Counters() {
		if strings.Contains(counter.Name(), metricName) {
			reason := ""
			if r, ok := counter.Tags()[reasonTag]; ok {
				reason = r
			}
			valuesByReason[reason] = int(counter.Value())
		}
	}
	return valuesByReason
}
