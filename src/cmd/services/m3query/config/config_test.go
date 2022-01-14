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

package config

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	xconfig "github.com/m3db/m3/src/x/config"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

const testConfigFile = "./testdata/config.yml"

func TestDefaultTagOptionsFromEmptyConfig(t *testing.T) {
	cfg := TagOptionsConfiguration{}
	opts, err := TagOptionsFromConfig(cfg)
	require.NoError(t, err)
	require.Equal(t, models.TypeQuoted, opts.IDSchemeType())
}

func TestTagOptionsFromConfigWithIDGenerationScheme(t *testing.T) {
	schemes := []models.IDSchemeType{
		models.TypePrependMeta,
		models.TypeQuoted,
	}
	for _, scheme := range schemes {
		cfg := TagOptionsConfiguration{
			Scheme: scheme,
		}

		opts, err := TagOptionsFromConfig(cfg)
		require.NoError(t, err)
		require.NotNil(t, opts)
		assert.Equal(t, []byte("__name__"), opts.MetricName())
		assert.Equal(t, scheme, opts.IDSchemeType())
	}
}

func TestTagOptionsFromConfig(t *testing.T) {
	name := "foobar"
	cfg := TagOptionsConfiguration{
		MetricName: name,
		Scheme:     models.TypeQuoted,
		Filters: []TagFilter{
			{Name: "foo", Values: []string{".", "abc"}},
			{Name: "bar", Values: []string{".*"}},
		},
	}
	opts, err := TagOptionsFromConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, opts)
	assert.Equal(t, []byte(name), opts.MetricName())
	filters := opts.Filters()
	exNames := [][]byte{[]byte("foo"), []byte("bar")}
	exVals := [][]string{{".", "abc"}, {".*"}}
	require.Equal(t, 2, len(filters))
	for i, f := range filters {
		assert.Equal(t, exNames[i], f.Name)
		for j, v := range f.Values {
			assert.Equal(t, []byte(exVals[i][j]), v)
		}
	}
}

func TestConfigLoading(t *testing.T) {
	var cfg Configuration
	require.NoError(t, xconfig.LoadFile(&cfg, testConfigFile, xconfig.Options{}))

	var requireExhaustive bool
	requireExhaustive = true
	assert.Equal(t, &LimitsConfiguration{
		PerQuery: PerQueryLimitsConfiguration{
			MaxFetchedSeries:  12000,
			MaxFetchedDocs:    11000,
			RequireExhaustive: &requireExhaustive,
		},
	}, &cfg.Limits)

	assert.Equal(t, HTTPConfiguration{EnableH2C: true}, cfg.HTTP)

	expectedTimestamp, err := time.Parse(time.RFC3339, "2022-01-01T00:00:00Z")
	require.NoError(t, err)
	expectedPromConvertOptions := storage.NewPromConvertOptions().
		SetResolutionThresholdForCounterNormalization(10 * time.Minute).
		SetValueDecreaseTolerance(0.0000000001).
		SetValueDecreaseToleranceUntil(xtime.UnixNano(expectedTimestamp.UnixNano()))
	actualPromConvertOptions := cfg.Query.Prometheus.ConvertOptionsOrDefault()
	assert.Equal(t, expectedPromConvertOptions, actualPromConvertOptions)

	// TODO: assert on more fields here.
}

func TestDefaultAsFetchOptionsBuilderLimitsOptions(t *testing.T) {
	limits := LimitsConfiguration{}
	assert.Equal(t, handleroptions.FetchOptionsBuilderLimitsOptions{
		SeriesLimit:            defaultStorageQuerySeriesLimit,
		InstanceMultiple:       float32(0),
		DocsLimit:              defaultStorageQueryDocsLimit,
		RangeLimit:             0,
		RequireExhaustive:      defaultRequireExhaustive,
		MaxMetricMetadataStats: defaultMaxMetricMetadataStats,
	}, limits.PerQuery.AsFetchOptionsBuilderLimitsOptions())
}

func TestConfigValidation(t *testing.T) {
	baseCfg := func(t *testing.T) *Configuration {
		var cfg Configuration
		require.NoError(t, xconfig.LoadFile(&cfg, testConfigFile, xconfig.Options{}),
			"sample configuration is no longer valid or loadable. Fix it up to provide a base config here")

		return &cfg
	}

	// limits configuration
	limitsCfgCases := []struct {
		name  string
		limit int
	}{{
		name:  "empty LimitsConfiguration is valid (implies disabled)",
		limit: 0,
	}, {
		name:  "LimitsConfiguration with positive limit is valid",
		limit: 5,
	}, {}, {
		name:  "LimitsConfiguration with negative limit is valid (implies disabled)",
		limit: -5,
	}}

	for _, tc := range limitsCfgCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseCfg(t)
			cfg.Limits = LimitsConfiguration{
				PerQuery: PerQueryLimitsConfiguration{
					MaxFetchedSeries: tc.limit,
				},
			}

			assert.NoError(t, validator.Validate(cfg))
		})
	}
}

func TestDefaultTagOptionsConfigErrors(t *testing.T) {
	var cfg TagOptionsConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(""), &cfg))
	opts, err := TagOptionsFromConfig(cfg)
	require.NoError(t, err)
	require.Equal(t, models.TypeQuoted, opts.IDSchemeType())
}

func TestGraphiteIDGenerationSchemeIsInvalid(t *testing.T) {
	var cfg TagOptionsConfiguration
	require.Error(t, yaml.Unmarshal([]byte("idScheme: graphite"), &cfg))
}

func TestTagOptionsConfigWithTagGenerationScheme(t *testing.T) {
	tests := []struct {
		schemeStr string
		scheme    models.IDSchemeType
	}{
		{"prepend_meta", models.TypePrependMeta},
		{"quoted", models.TypeQuoted},
	}

	for _, tt := range tests {
		var cfg TagOptionsConfiguration
		schemeConfig := fmt.Sprintf("idScheme: %s", tt.schemeStr)
		require.NoError(t, yaml.Unmarshal([]byte(schemeConfig), &cfg))
		opts, err := TagOptionsFromConfig(cfg)
		require.NoError(t, err)
		assert.Equal(t, []byte("__name__"), opts.MetricName())
		assert.Equal(t, tt.scheme, opts.IDSchemeType())
	}
}

func TestTagOptionsConfig(t *testing.T) {
	var cfg TagOptionsConfiguration
	config := "metricName: abcdefg\nidScheme: prepend_meta\nbucketName: foo"
	require.NoError(t, yaml.Unmarshal([]byte(config), &cfg))
	opts, err := TagOptionsFromConfig(cfg)
	require.NoError(t, err)
	assert.Equal(t, []byte("abcdefg"), opts.MetricName())
	assert.Equal(t, []byte("foo"), opts.BucketName())
	assert.Equal(t, models.TypePrependMeta, opts.IDSchemeType())
}

func TestKeepNaNsDefault(t *testing.T) {
	r := ResultOptions{
		KeepNaNs: true,
	}
	assert.Equal(t, true, r.KeepNaNs)

	r = ResultOptions{}
	assert.Equal(t, false, r.KeepNaNs)
}
