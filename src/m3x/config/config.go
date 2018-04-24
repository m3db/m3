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
	"io/ioutil"

	validator "gopkg.in/validator.v2"
	yaml "gopkg.in/yaml.v2"
)

var errNoFilesToLoad = errors.New("attempt to load config with no files")

// Options is an options set used when parsing config.
type Options struct {
	DisableUnmarshalStrict bool
	DisableValidate        bool
}

// LoadFile loads a config from a file.
func LoadFile(config interface{}, file string, opts Options) error {
	return LoadFiles(config, []string{file}, opts)
}

// LoadFiles loads a config from list of files. If value for a property is
// present in multiple files, the value from the last file will be applied.
// Validation is done after merging all values.
func LoadFiles(config interface{}, files []string, opts Options) error {
	if len(files) == 0 {
		return errNoFilesToLoad
	}
	for _, name := range files {
		data, err := ioutil.ReadFile(name)
		if err != nil {
			return err
		}
		unmarshal := yaml.UnmarshalStrict
		if opts.DisableUnmarshalStrict {
			unmarshal = yaml.Unmarshal
		}
		if err := unmarshal(data, config); err != nil {
			return err
		}
	}
	if opts.DisableValidate {
		return nil
	}
	return validator.Validate(config)
}
