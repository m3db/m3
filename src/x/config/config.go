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

// Package config provides utilities for loading configuration files.
package config

import (
	"errors"
	"io"
	"os"
	"reflect"
	"strings"

	"go.uber.org/config"
	"go.uber.org/zap"
	validator "gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

const (
	deprecatedPrefix = "Deprecated"
)

var (
	errNoFilesToLoad = errors.New("attempt to load config with no files")

	// osLookupEnv allows simple mocking of os.LookupEnv for the test of that
	// code path. Use Expand option
	// for most test cases instead.
	osLookupEnv = os.LookupEnv
)

// Options is an options set used when parsing config.
type Options struct {
	DisableUnmarshalStrict bool
	DisableValidate        bool

	// Expand provides values for templated strings of the form ${KEY}.
	// By default, we extract these values from the environment.
	Expand config.LookupFunc
}

// LoadFile loads a config from a file.
func LoadFile(dst interface{}, file string, opts Options) error {
	return LoadFiles(dst, []string{file}, opts)
}

// LoadFiles loads a config from list of files. If value for a property is
// present in multiple files, the value from the last file will be applied.
// Validation is done after merging all values.
func LoadFiles(dst interface{}, files []string, opts Options) error {
	if len(files) == 0 {
		return errNoFilesToLoad
	}

	yamlOpts := make([]config.YAMLOption, 0, len(files))
	for _, name := range files {
		yamlOpts = append(yamlOpts, config.File(name))
	}

	if opts.DisableUnmarshalStrict {
		yamlOpts = append(yamlOpts, config.Permissive())
	}

	expand := opts.Expand
	if expand == nil {
		expand = osLookupEnv
	}

	yamlOpts = append(yamlOpts, config.Expand(expand))

	provider, err := config.NewYAML(yamlOpts...)
	if err != nil {
		return err
	}

	if err := provider.Get(config.Root).Populate(dst); err != nil {
		return err
	}

	if opts.DisableValidate {
		return nil
	}

	return validator.Validate(dst)
}

// Dump writes the given configuration to stream dst as YAML.
func Dump(cfg interface{}, dst io.Writer) error {
	return yaml.NewEncoder(dst).Encode(cfg)
}

// deprecationCheck checks the config for deprecated fields and returns any in
// slice of strings.
func deprecationCheck(cfg interface{}, df []string) []string {
	n := reflect.TypeOf(cfg).NumField()
	for i := 0; i < n; i++ {
		v := reflect.ValueOf(cfg).Field(i)
		if v.Kind() == reflect.Struct {
			df = deprecationCheck(v.Interface(), df)
		}
		name := reflect.TypeOf(cfg).Field(i).Name
		if strings.HasPrefix(name, deprecatedPrefix) && !v.IsZero() {
			// Exclude unset deprecated config values from
			// raising warnings via checking IsZero.
			df = append(df, name)
		}
	}
	return df
}

// WarnOnDeprecation emits a warning for every deprecated field
func WarnOnDeprecation(cfg interface{}, logger *zap.Logger) {
	deprecatedFields := []string{}
	for _, v := range deprecationCheck(cfg, deprecatedFields) {
		logger.Warn("using deprecated configuration field", zap.String("field", v))
	}
}
