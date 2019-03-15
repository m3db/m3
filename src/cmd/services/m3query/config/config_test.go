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

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/cost"
	xdocs "github.com/m3db/m3/src/x/docs"
	xconfig "github.com/m3db/m3x/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/validator.v2"
	yaml "gopkg.in/yaml.v2"
)

const testConfigFile = "./testdata/config.yml"

func TestTagOptionsFromEmptyConfigErrors(t *testing.T) {
	cfg := TagOptionsConfiguration{}
	opts, err := TagOptionsFromConfig(cfg)
	require.Error(t, err)
	require.Nil(t, opts)
}

func TestTagOptionsFromConfigWithIDGenerationScheme(t *testing.T) {
	schemes := []models.IDSchemeType{models.TypeLegacy,
		models.TypePrependMeta, models.TypeQuoted}
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
		Scheme:     models.TypeLegacy,
	}
	opts, err := TagOptionsFromConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, opts)
	assert.Equal(t, []byte(name), opts.MetricName())
}

func TestLimitsConfiguration_AsLimitManagerOptions(t *testing.T) {
	cases := []struct {
		Input interface {
			AsLimitManagerOptions() cost.LimitManagerOptions
		}
		ExpectedDefault int64
	}{{
		Input: &PerQueryLimitsConfiguration{
			MaxFetchedDatapoints: 5,
		},
		ExpectedDefault: 5,
	}, {
		Input: &GlobalLimitsConfiguration{
			MaxFetchedDatapoints: 6,
		},
		ExpectedDefault: 6,
	}}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("type_%T", tc.Input), func(t *testing.T) {
			res := tc.Input.AsLimitManagerOptions()
			assert.Equal(t, cost.Limit{
				Threshold: cost.Cost(tc.ExpectedDefault),
				Enabled:   true,
			}, res.DefaultLimit())
		})
	}
}

func TestLimitsConfiguration_MaxComputedDatapoints(t *testing.T) {
	t.Run("uses PerQuery value if provided", func(t *testing.T) {
		lc := &LimitsConfiguration{
			DeprecatedMaxComputedDatapoints: 6,
			PerQuery: PerQueryLimitsConfiguration{
				PrivateMaxComputedDatapoints: 5,
			},
		}

		assert.Equal(t, int64(5), lc.MaxComputedDatapoints())
	})

	t.Run("uses deprecated value if PerQuery not provided", func(t *testing.T) {
		lc := &LimitsConfiguration{
			DeprecatedMaxComputedDatapoints: 6,
		}

		assert.Equal(t, int64(6), lc.MaxComputedDatapoints())
	})
}

func TestToLimitManagerOptions(t *testing.T) {
	cases := []struct {
		Name          string
		Input         int64
		ExpectedLimit cost.Limit
	}{{
		Name:  "negative is disabled",
		Input: -5,
		ExpectedLimit: cost.Limit{
			Threshold: cost.Cost(-5),
			Enabled:   false,
		},
	}, {
		Name:  "zero is disabled",
		Input: 0,
		ExpectedLimit: cost.Limit{
			Threshold: cost.Cost(0),
			Enabled:   false,
		},
	}, {
		Name:  "positive is enabled",
		Input: 5,
		ExpectedLimit: cost.Limit{
			Threshold: cost.Cost(5),
			Enabled:   true,
		},
	}}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			assert.Equal(t, tc.ExpectedLimit, toLimitManagerOptions(tc.Input).DefaultLimit())
		})
	}
}

func TestConfigLoading(t *testing.T) {
	var cfg Configuration
	require.NoError(t, xconfig.LoadFile(&cfg, testConfigFile, xconfig.Options{}))

	assert.Equal(t, &LimitsConfiguration{
		DeprecatedMaxComputedDatapoints: 10555,
		PerQuery: PerQueryLimitsConfiguration{
			PrivateMaxComputedDatapoints: 12000,
			MaxFetchedDatapoints:         11000,
		},
		Global: GlobalLimitsConfiguration{
			MaxFetchedDatapoints: 13000,
		},
	}, &cfg.Limits)
	// TODO: assert on more fields here.
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
		Name  string
		Limit int64
	}{{
		Name:  "empty LimitsConfiguration is valid (implies disabled)",
		Limit: 0,
	}, {
		Name:  "LimitsConfiguration with positive limit is valid",
		Limit: 5,
	}, {}, {
		Name:  "LimitsConfiguration with negative limit is valid (implies disabled)",
		Limit: -5,
	}}

	for _, tc := range limitsCfgCases {
		t.Run(tc.Name, func(t *testing.T) {
			cfg := baseCfg(t)
			cfg.Limits = LimitsConfiguration{
				PerQuery: PerQueryLimitsConfiguration{
					PrivateMaxComputedDatapoints: tc.Limit,
				}}

			assert.NoError(t, validator.Validate(cfg))
		})
	}
}

func TestDefaultTagOptionsConfigErrors(t *testing.T) {
	var cfg TagOptionsConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(""), &cfg))
	opts, err := TagOptionsFromConfig(cfg)

	docLink := xdocs.Path("how_to/query#migration")
	expectedError := fmt.Sprintf(errNoIDGenerationScheme, docLink)
	require.EqualError(t, err, expectedError)
	require.Nil(t, opts)
}

func TestGraphiteIDGenerationSchemeIsInvalid(t *testing.T) {
	var cfg TagOptionsConfiguration
	require.Error(t, yaml.Unmarshal([]byte("idScheme: graphite"), &cfg))
}

func TestTagOptionsConfigWithTagGenerationScheme(t *testing.T) {
	var tests = []struct {
		schemeStr string
		scheme    models.IDSchemeType
	}{
		{"legacy", models.TypeLegacy},
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

func TestNegativeQueryConversionSize(t *testing.T) {
	size := -2
	q := QueryConversionCacheConfiguration{
		Size: &size,
	}

	err := q.Validate()
	require.Error(t, err)
}

func TestNilQueryConversionSize(t *testing.T) {
	q := &QueryConversionCacheConfiguration{}

	err := q.Validate()
	require.NoError(t, err)
}

func TestKeepNaNsDefault(t *testing.T) {
	r := ResultOptions{
		KeepNans: true,
	}
	assert.Equal(t, true, r.KeepNans)

	r = ResultOptions{}
	assert.Equal(t, false, r.KeepNans)
}
