package rules

import (
	"testing"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/metric"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestValidationConfigurationRequiredRollupTags(t *testing.T) {
	cfg := `
requiredRollupTags:
  - tag1
  - tag2
`
	var c ValidationConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	require.Equal(t, []string{"tag1", "tag2"}, c.RequiredRollupTags)
}

func TestValidationConfigurationTagNameInvalidChars(t *testing.T) {
	cfg := `tagNameInvalidChars: "%\n"`
	var c ValidationConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	require.Equal(t, []rune{'%', '\n'}, toRunes(c.TagNameInvalidChars))
}

func TestValidationConfigurationMetricNameInvalidChars(t *testing.T) {
	cfg := `metricNameInvalidChars: "%\n"`
	var c ValidationConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	require.Equal(t, []rune{'%', '\n'}, toRunes(c.MetricNameInvalidChars))
}

func TestNewMetricTypesFn(t *testing.T) {
	cfg := `
typeTag: type
allowed:
  - counter
  - timer
  - gauge
`

	var c metricTypesValidationConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	fn := c.NewMetricTypesFn()

	inputs := []struct {
		filters       filters.TagFilterValueMap
		expectedTypes []metric.Type
	}{
		{
			filters:       nil,
			expectedTypes: []metric.Type{metric.CounterType, metric.TimerType, metric.GaugeType},
		},
		{
			filters: filters.TagFilterValueMap{
				"randomTag": filters.FilterValue{Pattern: "counter"},
			},
			expectedTypes: []metric.Type{metric.CounterType, metric.TimerType, metric.GaugeType},
		},
		{
			filters: filters.TagFilterValueMap{
				"type": filters.FilterValue{Pattern: "counter"},
			},
			expectedTypes: []metric.Type{metric.CounterType},
		},
		{
			filters: filters.TagFilterValueMap{
				"type": filters.FilterValue{Pattern: "timer"},
			},
			expectedTypes: []metric.Type{metric.TimerType},
		},
		{
			filters: filters.TagFilterValueMap{
				"type": filters.FilterValue{Pattern: "gauge"},
			},
			expectedTypes: []metric.Type{metric.GaugeType},
		},
		{
			filters: filters.TagFilterValueMap{
				"type": filters.FilterValue{Pattern: "*er"},
			},
			expectedTypes: []metric.Type{metric.CounterType, metric.TimerType},
		},
	}

	for _, input := range inputs {
		res, err := fn(input.filters)
		require.NoError(t, err)
		require.Equal(t, input.expectedTypes, res)
	}
}

func TestNewMetricTypesFnError(t *testing.T) {
	cfg := `
typeTag: type
allowed:
  - counter
  - timer
  - gauge
`

	var c metricTypesValidationConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &c))
	fn := c.NewMetricTypesFn()

	inputs := []filters.TagFilterValueMap{
		filters.TagFilterValueMap{
			"type": filters.FilterValue{Pattern: "a[b"},
		},
		filters.TagFilterValueMap{
			"type": filters.FilterValue{Pattern: "ab{"},
		},
	}
	for _, input := range inputs {
		res, err := fn(input)
		require.Error(t, err)
		require.Nil(t, res)
	}
}

func TestToRunes(t *testing.T) {
	s := "%\n 6s[:\\"
	require.Equal(t, []rune{'%', '\n', ' ', '6', 's', '[', ':', '\\'}, toRunes(s))
}
