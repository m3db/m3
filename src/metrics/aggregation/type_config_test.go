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

package aggregation

import (
	"strings"
	"testing"

	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestTypesConfiguration(t *testing.T) {
	str := `
defaultGaugeAggregationTypes: Max
defaultTimerAggregationTypes: P50,P99,P9999
globalOverrides:
  Mean: testMean
gaugeOverrides:
  Last: ""
counterOverrides:
  Sum: ""
transformFnType: suffix
`

	var cfg TypesConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
	opts, err := cfg.NewOptions(instrument.NewOptions())
	require.NoError(t, err)
	require.Equal(t, defaultDefaultCounterAggregationTypes, opts.DefaultCounterAggregationTypes())
	require.Equal(t, Types{Max}, opts.DefaultGaugeAggregationTypes())
	require.Equal(t, Types{P50, P99, P9999}, opts.DefaultTimerAggregationTypes())
	require.Equal(t, []byte(".testMean"), opts.TypeStringForCounter(Mean))
	require.Equal(t, []byte(nil), opts.TypeStringForCounter(Sum))
	require.Equal(t, []byte(nil), opts.TypeStringForGauge(Last))
	typeStrings := opts.DefaultTimerAggregationTypeStrings()
	for i, aggType := range opts.DefaultTimerAggregationTypes() {
		require.Equal(t, typeStrings[i], opts.TypeStringForTimer(aggType))
		require.True(t, strings.HasPrefix(string(typeStrings[i]), "."))
	}
}

func TestTypesConfigurationNoTransformFnType(t *testing.T) {
	str := `
defaultGaugeAggregationTypes: Max
defaultTimerAggregationTypes: P50,P99,P9999
globalOverrides:
  Mean: testMean
gaugeOverrides:
  Last: ""
counterOverrides:
  Sum: ""
`

	var cfg TypesConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
	_, err := cfg.NewOptions(instrument.NewOptions())
	require.NoError(t, err)
}

func TestTypesConfigurationError(t *testing.T) {
	str := `
defaultGaugeAggregationTypes: Max
defaultTimerAggregationTypes: P50,P99,P9999
transformFnType: bla
`

	var cfg TypesConfiguration
	require.Error(t, yaml.Unmarshal([]byte(str), &cfg))
}
