// Copyright (c) 2019 Uber Technologies, Inc.
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

package configflag

import (
	"bytes"
	"flag"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/m3db/m3/src/x/config"
)

type testConfig struct {
	Foo int    `yaml:"foo"`
	Bar string `yaml:"bar"`
}

func TestCommandLineOptions(t *testing.T) {
	type testContext struct {
		Flags       flag.FlagSet
		Opts        Options
		MockOS      *MockosIface
		UsageCalled bool
	}

	setup := func(t *testing.T) (*testContext, func()) {
		ctrl := gomock.NewController(t)
		mockOS := NewMockosIface(ctrl)

		tctx := &testContext{
			Opts:   Options{osFns: mockOS},
			MockOS: mockOS,
		}

		tctx.Flags.Usage = func() {
			tctx.UsageCalled = true
		}

		tctx.Opts.RegisterFlagSet(&tctx.Flags)

		return tctx, ctrl.Finish
	}

	expectedConfig := testConfig{Foo: 1, Bar: "bar"}
	configFileOpts := []string{"-f", "./testdata/config1.yaml", "-f", "./testdata/config2.yaml"}

	configSecretFileOpts := []string{"-c", "./testdata/config1.yaml", "-c", "./testdata/config2.yaml"}

	t.Run("loads config from files", func(t *testing.T) {
		tctx, teardown := setup(t)
		defer teardown()

		require.NoError(t, tctx.Flags.Parse(configFileOpts))

		var cfg testConfig
		require.NoError(t, tctx.Opts.MainLoad(&cfg, config.Options{}))
		assert.Equal(t, expectedConfig, cfg)
	})

	t.Run("dumps config and exits on d", func(t *testing.T) {
		tctx, teardown := setup(t)
		defer teardown()

		stdout := &bytes.Buffer{}

		tctx.MockOS.EXPECT().Exit(0).Times(1)
		tctx.MockOS.EXPECT().Stdout().Return(stdout)

		args := append([]string{"-d"}, configFileOpts...)
		require.NoError(t, tctx.Flags.Parse(args))

		var cfg testConfig
		require.NoError(t, tctx.Opts.MainLoad(&cfg, config.Options{}))

		var actual testConfig
		require.NoError(t, yaml.NewDecoder(bytes.NewReader(stdout.Bytes())).Decode(&actual))
		assert.Equal(t, expectedConfig, actual)
	})

	t.Run("errors on no configs", func(t *testing.T) {
		tctx, teardown := setup(t)
		defer teardown()

		require.NoError(t, tctx.Flags.Parse(nil))

		require.EqualError(t,
			tctx.Opts.MainLoad(nil, config.Options{}),
			"-f is required (no config files provided)")
		assert.True(t, tctx.UsageCalled)
	})
	t.Run("loads yaml files from -c flag", func(t *testing.T) {
		tctx, teardown := setup(t)
		defer teardown()

		require.NoError(t, tctx.Flags.Parse(configSecretFileOpts))

		var cfg testConfig
		_, err := tctx.Opts.CredentialLoad(&cfg, config.Options{})
		require.NoError(t, err)
		assert.Equal(t, expectedConfig, cfg)
	})

	t.Run("loads yaml files from -c flag", func(t *testing.T) {
		tctx, teardown := setup(t)
		defer teardown()

		require.NoError(t, tctx.Flags.Parse([]string{}))

		var cfg testConfig
		isPresent, err := tctx.Opts.CredentialLoad(&cfg, config.Options{})
		require.NoError(t, err)
		require.False(t, isPresent)
	})

}

func TestFlagArray(t *testing.T) {
	tests := []struct {
		args     []string
		defaults FlagStringSlice
		name     string
		want     string
	}{
		{
			name: "single value",
			args: []string{"-f", "./some/file/path/here.yaml"},
			want: "[./some/file/path/here.yaml]",
		},
		{
			name: "single empty value",
			args: []string{"-f", ""},
			want: "[]",
		},
		{
			name: "two value",
			args: []string{"-f", "file1.yaml", "-f", "file2.yaml"},
			want: "[file1.yaml file2.yaml]",
		},
		{
			name: "two value one of which empty",
			args: []string{"-f", "", "-f", "file2.yaml"},
			want: "[ file2.yaml]",
		},
		{
			name: "three values",
			args: []string{"-f", "file1.yaml", "-f", "file2.yaml", "-f", "file3.yaml"},
			want: "[file1.yaml file2.yaml file3.yaml]",
		},
		{
			name:     "default and no flag",
			args:     nil,
			want:     "[file1.yaml]",
			defaults: FlagStringSlice{Value: []string{"file1.yaml"}},
		},
		{
			name:     "default is overridden by flag",
			args:     []string{"-f", "override.yaml"},
			want:     "[override.yaml]",
			defaults: FlagStringSlice{Value: []string{"file1.yaml"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := flag.NewFlagSet(tt.name, flag.PanicOnError)
			fs.Var(&tt.defaults, "f", "config files")
			err := fs.Parse(tt.args)
			require.NoError(t, err, "error parsing flags")
			assert.Equal(t, tt.want, tt.defaults.String(), "unexpected output")
		})
	}
}

func TestFlagStringSliceNilFlag(t *testing.T) {
	var s *FlagStringSlice
	assert.Equal(t, "", s.String(), "nil string slice representation")
}

func TestFlagStringSliceWithOtherFlags(t *testing.T) {
	var values FlagStringSlice
	var x string
	fs := flag.NewFlagSet("app", flag.PanicOnError)
	fs.StringVar(&x, "x", "", "some random var")
	fs.Var(&values, "f", "config files")
	require.NoError(t, fs.Parse([]string{"-f", "file1.yaml", "-x", "file2.yaml", "-f", "file3.yaml"}))
	assert.Equal(t, "[file1.yaml file3.yaml]", values.String(), "flag string slice representation")
	assert.Equal(t, "file2.yaml", x, "x value")
}
