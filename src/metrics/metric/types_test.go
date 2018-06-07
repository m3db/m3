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

package metric

import (
	"testing"

	"github.com/m3db/m3metrics/generated/proto/metricpb"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestTypeUnmarshalYAML(t *testing.T) {
	inputs := []struct {
		str      string
		expected Type
	}{
		{str: "counter", expected: CounterType},
		{str: "timer", expected: TimerType},
		{str: "gauge", expected: GaugeType},
	}
	for _, input := range inputs {
		var typ Type
		require.NoError(t, yaml.Unmarshal([]byte(input.str), &typ))
		require.Equal(t, input.expected, typ)
	}
}

func TestTypeUnmarshalYAMLErrors(t *testing.T) {
	inputs := []string{
		"huh",
		"blah",
	}
	for _, input := range inputs {
		var typ Type
		err := yaml.Unmarshal([]byte(input), &typ)
		require.Error(t, err)
		require.Equal(t, "invalid metric type '"+input+"', valid types are: counter, timer, gauge", err.Error())
	}
}

func TestMustParseType(t *testing.T) {
	require.Equal(t, CounterType, MustParseType("counter"))
	require.Panics(t, func() { MustParseType("foo") })
}

func TestTypeToProto(t *testing.T) {
	inputs := []struct {
		metricType Type
		expected   metricpb.MetricType
	}{
		{
			metricType: CounterType,
			expected:   metricpb.MetricType_COUNTER,
		},
		{
			metricType: TimerType,
			expected:   metricpb.MetricType_TIMER,
		},
		{
			metricType: GaugeType,
			expected:   metricpb.MetricType_GAUGE,
		},
	}

	for _, input := range inputs {
		var mt metricpb.MetricType
		require.NoError(t, input.metricType.ToProto(&mt))
		require.Equal(t, input.expected, mt)
	}
}

func TestTypeToProtoBadType(t *testing.T) {
	inputs := []Type{
		UnknownType,
		Type(1000),
	}

	for _, input := range inputs {
		var mt metricpb.MetricType
		require.Error(t, input.ToProto(&mt))
	}
}

func TestTypeFromProto(t *testing.T) {
	inputs := []struct {
		metricType metricpb.MetricType
		expected   Type
	}{
		{
			metricType: metricpb.MetricType_COUNTER,
			expected:   CounterType,
		},
		{
			metricType: metricpb.MetricType_TIMER,
			expected:   TimerType,
		},
		{
			metricType: metricpb.MetricType_GAUGE,
			expected:   GaugeType,
		},
	}

	var mt Type
	for _, input := range inputs {
		require.NoError(t, mt.FromProto(input.metricType))
		require.Equal(t, input.expected, mt)
	}
}

func TestTypeFromProtoBadTypeProto(t *testing.T) {
	inputs := []metricpb.MetricType{
		metricpb.MetricType_UNKNOWN,
		metricpb.MetricType(1000),
	}

	for _, input := range inputs {
		var mt Type
		require.Error(t, mt.FromProto(input))
	}
}
