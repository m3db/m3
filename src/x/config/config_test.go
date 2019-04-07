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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestLoadFile(t *testing.T) {
	var cfg configuration

	err := LoadFile(&cfg, "./no-config.yaml", Options{})
	require.Error(t, err)

	// invalid yaml file
	err = LoadFile(&cfg, "./config.go", Options{})
	require.Error(t, err)

	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

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
	defer os.Remove(fname)

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
	defer os.Remove(fname)

	err := LoadFile(&cfg, fname, Options{})
	require.Error(t, err)
}

func TestLoadFileInvalidKeyDisableMarshalStrict(t *testing.T) {
	var cfg configuration

	fname := writeFile(t, badConfigInvalidKey)
	defer os.Remove(fname)

	err := LoadFile(&cfg, fname, Options{DisableUnmarshalStrict: true})
	require.NoError(t, err)
	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 1024, cfg.BufferSpace)
	require.Equal(t, []string{"server1:8090", "server2:8010"}, cfg.Servers)
}

func TestLoadFileInvalidValue(t *testing.T) {
	var cfg configuration

	fname := writeFile(t, badConfigInvalidValue)
	defer os.Remove(fname)

	err := LoadFile(&cfg, fname, Options{})
	require.Error(t, err)
}

func TestLoadFileInvalidValueDisableValidate(t *testing.T) {
	var cfg configuration

	fname := writeFile(t, badConfigInvalidValue)
	defer os.Remove(fname)

	err := LoadFile(&cfg, fname, Options{DisableValidate: true})
	require.NoError(t, err)
	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 254, cfg.BufferSpace)
	require.Equal(t, []string{"server1:8090", "server2:8010"}, cfg.Servers)
}

func TestLoadFilesExtends(t *testing.T) {
	fname := writeFile(t, goodConfig)
	defer os.Remove(fname)

	partialConfig := `
buffer_space: 8080
servers:
    - server3:8080
    - server4:8080
`
	partial := writeFile(t, partialConfig)
	defer os.Remove(partial)

	var cfg configuration
	err := LoadFiles(&cfg, []string{fname, partial}, Options{})
	require.NoError(t, err)

	require.Equal(t, "localhost:4385", cfg.ListenAddress)
	require.Equal(t, 8080, cfg.BufferSpace)
	require.Equal(t, []string{"server3:8080", "server4:8080"}, cfg.Servers)
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
	defer os.Remove(fname1)

	fname2 := writeFile(t, invalidConfig2)
	defer os.Remove(invalidConfig2)

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

func writeFile(t *testing.T, contents string) string {
	f, err := ioutil.TempFile("", "configtest")
	require.NoError(t, err)

	defer f.Close()

	_, err = f.Write([]byte(contents))
	require.NoError(t, err)

	return f.Name()
}
