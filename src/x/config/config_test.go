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

package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/config"
)

const (
	goodConfig = `
listen_address: localhost:4385
buffer_space: 1024
servers:
    - server1:8090
    - server2:8010
`
	badConfigInvalidKey = `
unknown_key: unknown_key_value
listen_address: localhost:4385
buffer_space: 1024
servers:
    - server1:8090
    - server2:8010
`
	badConfigInvalidValue = `
listen_address: localhost:4385
buffer_space: 254
servers:
    - server1:8090
    - server2:8010
`
)

type configuration struct {
	ListenAddress string   `yaml:"listen_address" validate:"nonzero"`
	BufferSpace   int      `yaml:"buffer_space" validate:"min=255"`
	Servers       []string `validate:"nonzero"`
}

type commitlogPolicyConfiguration struct {
	FlushMaxBytes       int    `yaml:"flushMaxBytes" validate:"nonzero"`
	FlushEvery          string `yaml:"flushEvery" validate:"nonzero"`
	DeprecatedBlockSize int    `yaml:"blockSize"`
}

type configurationDeprecated struct {
	ListenAddress string   `yaml:"listen_address" validate:"nonzero"`
	BufferSpace   int      `yaml:"buffer_space" validate:"min=255"`
	Servers       []string `validate:"nonzero"`
	DeprecatedFoo string   `yaml:"foo"`
	DeprecatedBar int      `yaml:"bar"`
	DeprecatedBaz *int     `yaml:"baz"`
}

type nestedConfigurationDeprecated struct {
	ListenAddress string                       `yaml:"listen_address" validate:"nonzero"`
	BufferSpace   int                          `yaml:"buffer_space" validate:"min=255"`
	Servers       []string                     `validate:"nonzero"`
	CommitLog     commitlogPolicyConfiguration `yaml:"commitlog"`
}

type nestedConfigurationMultipleDeprecated struct {
	ListenAddress      string                       `yaml:"listen_address" validate:"nonzero"`
	BufferSpace        int                          `yaml:"buffer_space" validate:"min=255"`
	Servers            []string                     `validate:"nonzero"`
	CommitLog          commitlogPolicyConfiguration `yaml:"commitlog"`
	DeprecatedMultiple configurationDeprecated      `yaml:"multiple"`
}

func TestLoadFile(t *testing.T) {
	var cfg configuration

	err := LoadFile(&cfg, "./no-config.yaml", Options{})
	require.Error(t, err)

	// invalid yaml file
	err = LoadFile(&cfg, "./config.go", Options{})
	require.Error(t, err)

	fname := writeFile(t, goodConfig)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	err = LoadFile(&cfg, fname, Options{})
	require.NoError(t, err)
	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 1024, cfg.BufferSpace)
	require.Equal(t, []string{"server1:8090", "server2:8010"}, cfg.Servers)
}

func TestLoadWithInvalidFile(t *testing.T) {
	var cfg configuration

	// no file provided
	err := LoadFiles(&cfg, nil, Options{})
	require.Error(t, err)
	require.Equal(t, errNoFilesToLoad, err)

	// non-exist file provided
	err = LoadFiles(&cfg, []string{"./no-config.yaml"}, Options{})
	require.Error(t, err)

	// invalid yaml file
	err = LoadFiles(&cfg, []string{"./config.go"}, Options{})
	require.Error(t, err)

	fname := writeFile(t, goodConfig)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	// non-exist file in the file list
	err = LoadFiles(&cfg, []string{fname, "./no-config.yaml"}, Options{})
	require.Error(t, err)

	// invalid file in the file list
	err = LoadFiles(&cfg, []string{fname, "./config.go"}, Options{})
	require.Error(t, err)
}

func TestLoadFileInvalidKey(t *testing.T) {
	var cfg configuration

	fname := writeFile(t, badConfigInvalidKey)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	err := LoadFile(&cfg, fname, Options{})
	require.Error(t, err)
}

func TestLoadFileInvalidKeyDisableMarshalStrict(t *testing.T) {
	var cfg configuration

	fname := writeFile(t, badConfigInvalidKey)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	err := LoadFile(&cfg, fname, Options{DisableUnmarshalStrict: true})
	require.NoError(t, err)
	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 1024, cfg.BufferSpace)
	require.Equal(t, []string{"server1:8090", "server2:8010"}, cfg.Servers)
}

func TestLoadFileInvalidValue(t *testing.T) {
	var cfg configuration

	fname := writeFile(t, badConfigInvalidValue)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	err := LoadFile(&cfg, fname, Options{})
	require.Error(t, err)
}

func TestLoadFileInvalidValueDisableValidate(t *testing.T) {
	var cfg configuration

	fname := writeFile(t, badConfigInvalidValue)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	err := LoadFile(&cfg, fname, Options{DisableValidate: true})
	require.NoError(t, err)
	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 254, cfg.BufferSpace)
	require.Equal(t, []string{"server1:8090", "server2:8010"}, cfg.Servers)
}

func TestLoadFilesExtends(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer func() {
		require.NoError(t, os.Remove(fname))
	}()

	partialConfig := `
buffer_space: 8080
servers:
    - server3:8080
    - server4:8080
`
	partial := writeFile(t, partialConfig)
	defer func() {
		require.NoError(t, os.Remove(partial))
	}()

	var cfg configuration
	err := LoadFiles(&cfg, []string{fname, partial}, Options{})
	require.NoError(t, err)

	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 8080, cfg.BufferSpace)
	require.Equal(t, []string{"server3:8080", "server4:8080"}, cfg.Servers)
}

func TestLoadFilesDeepExtends(t *testing.T) {
	type innerConfig struct {
		K1 string `yaml:"k1"`
		K2 string `yaml:"k2"`
	}

	type nestedConfig struct {
		Foo innerConfig `yaml:"foo"`
	}

	const (
		base = `
foo:
  k1: v1_base
  k2: v2_base
`
		override = `
foo:
  k1: v1_override
`
	)

	baseFile, overrideFile := writeFile(t, base), writeFile(t, override)

	var cfg nestedConfig
	require.NoError(t, LoadFiles(&cfg, []string{baseFile, overrideFile}, Options{}))

	assert.Equal(t, nestedConfig{
		Foo: innerConfig{
			K1: "v1_override",
			K2: "v2_base",
		},
	}, cfg)

}

func TestLoadFilesValidateOnce(t *testing.T) {
	const invalidConfig1 = `
    listen_address:
    buffer_space: 256
    servers:
    `

	const invalidConfig2 = `
    listen_address: "localhost:8080"
    servers:
      - server2:8010
    `

	fname1 := writeFile(t, invalidConfig1)
	defer func() {
		require.NoError(t, os.Remove(fname1))
	}()

	fname2 := writeFile(t, invalidConfig2)
	defer func() {
		require.NoError(t, os.Remove(fname2))
	}()

	// Either config by itself will not pass validation.
	var cfg1 configuration
	err := LoadFiles(&cfg1, []string{fname1}, Options{})
	require.Error(t, err)

	var cfg2 configuration
	err = LoadFiles(&cfg2, []string{fname2}, Options{})
	require.Error(t, err)

	// But merging load has no error.
	var mergedCfg configuration
	err = LoadFiles(&mergedCfg, []string{fname1, fname2}, Options{})
	require.NoError(t, err)

	require.Equal(t, "localhost:8080", mergedCfg.ListenAddress)
	require.Equal(t, 256, mergedCfg.BufferSpace)
	require.Equal(t, []string{"server2:8010"}, mergedCfg.Servers)
}

func TestLoadFilesEnvExpansion(t *testing.T) {
	const withEnv = `
    listen_address: localhost:${PORT:8080}
    buffer_space: ${BUFFER_SPACE}
    servers:
      - server2:8010
    `

	mapLookup := func(m map[string]string) config.LookupFunc {
		return func(s string) (string, bool) {
			r, ok := m[s]
			return r, ok
		}
	}

	newMapLookupOptions := func(m map[string]string) Options {
		return Options{
			Expand: mapLookup(m),
		}
	}

	type testCase struct {
		Name        string
		Options     Options
		Expected    configuration
		ExpectedErr error
	}

	cases := []testCase{{
		Name: "all_provided",
		Options: newMapLookupOptions(map[string]string{
			"PORT":         "9090",
			"BUFFER_SPACE": "256",
		}),
		Expected: configuration{
			ListenAddress: "localhost:9090",
			BufferSpace:   256,
			Servers:       []string{"server2:8010"},
		},
	}, {
		Name: "missing_no_default",
		Options: newMapLookupOptions(map[string]string{
			"PORT": "9090",
			// missing BUFFER_SPACE,
		}),
		ExpectedErr: errors.New("couldn't expand environment: default is empty for \"BUFFER_SPACE\" (use \"\" for empty string)"),
	}, {
		Name: "missing_with_default",
		Options: newMapLookupOptions(map[string]string{
			"BUFFER_SPACE": "256",
		}),
		Expected: configuration{
			ListenAddress: "localhost:8080",
			BufferSpace:   256,
			Servers:       []string{"server2:8010"},
		},
	}}

	doTest := func(t *testing.T, tc testCase) {
		fname1 := writeFile(t, withEnv)
		defer func() {
			require.NoError(t, os.Remove(fname1))
		}()
		var cfg configuration

		err := LoadFile(&cfg, fname1, tc.Options)
		if tc.ExpectedErr != nil {
			require.EqualError(t, err, tc.ExpectedErr.Error())
			return
		}

		require.NoError(t, err)
		assert.Equal(t, tc.Expected, cfg)
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			doTest(t, tc)
		})
	}

	t.Run("uses os.LookupEnv by default", func(t *testing.T) {
		curOSLookupEnv := osLookupEnv
		osLookupEnv = mapLookup(map[string]string{
			"PORT":         "9090",
			"BUFFER_SPACE": "256",
		})
		defer func() {
			osLookupEnv = curOSLookupEnv
		}()

		doTest(t, testCase{
			// use defaults
			Options: Options{},
			Expected: configuration{
				ListenAddress: "localhost:9090",
				BufferSpace:   256,
				Servers:       []string{"server2:8010"},
			},
		})
	})
}

func TestDeprecationCheck(t *testing.T) {
	t.Run("StandardConfig", func(t *testing.T) {
		// OK
		var cfg configuration
		fname := writeFile(t, goodConfig)
		defer func() {
			require.NoError(t, os.Remove(fname))
		}()

		err := LoadFile(&cfg, fname, Options{})
		require.NoError(t, err)

		df := []string{}
		ss := deprecationCheck(cfg, df)
		require.Len(t, ss, 0)

		// Deprecated
		badConfig := `
listen_address: localhost:4385
buffer_space: 1024
servers:
  - server1:8090
  - server2:8010
foo: ok
bar: 42
`
		var cfg2 configurationDeprecated
		fname2 := writeFile(t, badConfig)
		defer func() {
			require.NoError(t, os.Remove(fname2))
		}()

		err = LoadFile(&cfg2, fname2, Options{})
		require.NoError(t, err)

		actual := deprecationCheck(cfg2, df)
		expect := []string{"DeprecatedFoo", "DeprecatedBar"}
		require.Equal(t, len(expect), len(actual),
			fmt.Sprintf("expect %#v should be equal actual %#v", expect, actual))
		require.Equal(t, expect, actual,
			fmt.Sprintf("expect %#v should be equal actual %#v", expect, actual))
	})

	t.Run("DeprecatedZeroValue", func(t *testing.T) {
		// OK
		var cfg configuration
		fname := writeFile(t, goodConfig)
		defer func() {
			require.NoError(t, os.Remove(fname))
		}()

		err := LoadFile(&cfg, fname, Options{})
		require.NoError(t, err)

		df := []string{}
		ss := deprecationCheck(cfg, df)
		require.Equal(t, 0, len(ss))

		// Deprecated zero value should be ok and not printed
		badConfig := `
listen_address: localhost:4385
buffer_space: 1024
servers:
  - server1:8090
  - server2:8010
foo: ok
bar: 42
baz: null
`
		var cfg2 configurationDeprecated
		fname2 := writeFile(t, badConfig)
		defer func() {
			require.NoError(t, os.Remove(fname2))
		}()

		err = LoadFile(&cfg2, fname2, Options{})
		require.NoError(t, err)

		actual := deprecationCheck(cfg2, df)
		expect := []string{"DeprecatedFoo", "DeprecatedBar"}
		require.Equal(t, len(expect), len(actual),
			fmt.Sprintf("expect %#v should be equal actual %#v", expect, actual))
		require.Equal(t, expect, actual,
			fmt.Sprintf("expect %#v should be equal actual %#v", expect, actual))
	})

	t.Run("DeprecatedNilValue", func(t *testing.T) {
		// OK
		var cfg configuration
		fname := writeFile(t, goodConfig)
		defer func() {
			require.NoError(t, os.Remove(fname))
		}()

		err := LoadFile(&cfg, fname, Options{})
		require.NoError(t, err)

		df := []string{}
		ss := deprecationCheck(cfg, df)
		require.Equal(t, 0, len(ss))

		// Deprecated nil/unset value should be ok and not printed
		validConfig := `
listen_address: localhost:4385
buffer_space: 1024
servers:
  - server1:8090
  - server2:8010
`
		var cfg2 configurationDeprecated
		fname2 := writeFile(t, validConfig)
		defer func() {
			require.NoError(t, os.Remove(fname2))
		}()

		err = LoadFile(&cfg2, fname2, Options{})
		require.NoError(t, err)

		actual := deprecationCheck(cfg2, df)
		require.Equal(t, 0, len(actual),
			fmt.Sprintf("expect %#v should be equal actual %#v", 0, actual))
	})

	t.Run("NestedConfig", func(t *testing.T) {
		// Single Deprecation
		var cfg nestedConfigurationDeprecated
		nc := `
listen_address: localhost:4385
buffer_space: 1024
servers:
  - server1:8090
  - server2:8010
commitlog:
  flushMaxBytes: 42
  flushEvery: second
  blockSize: 23
`
		fname := writeFile(t, nc)
		defer func() {
			require.NoError(t, os.Remove(fname))
		}()

		err := LoadFile(&cfg, fname, Options{})
		require.NoError(t, err)

		df := []string{}
		actual := deprecationCheck(cfg, df)
		expect := []string{"DeprecatedBlockSize"}
		require.Equal(t, len(expect), len(actual),
			fmt.Sprintf("expect %#v should be equal actual %#v", expect, actual))
		require.Equal(t, expect, actual,
			fmt.Sprintf("expect %#v should be equal actual %#v", expect, actual))

		// Multiple deprecation
		var cfg2 nestedConfigurationMultipleDeprecated
		nc = `
listen_address: localhost:4385
buffer_space: 1024
servers:
  - server1:8090
  - server2:8010
commitlog:
  flushMaxBytes: 42
  flushEvery: second
multiple:
  listen_address: localhost:4385
  buffer_space: 1024
  servers:
    - server1:8090
    - server2:8010
  foo: ok
  bar: 42
`

		fname2 := writeFile(t, nc)
		defer func() {
			require.NoError(t, os.Remove(fname2))
		}()

		err = LoadFile(&cfg2, fname2, Options{})
		require.NoError(t, err)

		df = []string{}
		actual = deprecationCheck(cfg2, df)
		expect = []string{
			"DeprecatedMultiple",
			"DeprecatedFoo",
			"DeprecatedBar",
		}
		require.Equal(t, len(expect), len(actual),
			fmt.Sprintf("expect %#v should be equal actual %#v", expect, actual))
		require.True(t, slicesContainSameStrings(expect, actual),
			fmt.Sprintf("expect %#v should be equal actual %#v", expect, actual))
	})
}

func slicesContainSameStrings(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}

	m := make(map[string]bool, len(s1))
	for _, v := range s1 {
		m[v] = true
	}
	for _, v := range s2 {
		if _, ok := m[v]; !ok {
			return false
		}
	}
	return true
}

func writeFile(t *testing.T, contents string) string {
	f, err := ioutil.TempFile("", "configtest")
	require.NoError(t, err)

	defer f.Close()

	_, err = f.Write([]byte(contents))
	require.NoError(t, err)

	return f.Name()
}
