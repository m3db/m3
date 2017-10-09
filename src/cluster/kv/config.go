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

package kv

import "github.com/m3db/m3x/log"

// Configuration is the config for kv options.
type Configuration struct {
	Logging     *log.Configuration `yaml:"logging"`
	Environment string             `yaml:"environment"`
	Namespace   string             `yaml:"namespace"`
}

// NewOptions creates a kv.Options.
func (cfg Configuration) NewOptions() (Options, error) {
	var logger log.Logger
	if cfg.Logging != nil {
		var err error
		logger, err = cfg.Logging.BuildLogger()
		if err != nil {
			return nil, err
		}
	}

	return NewOptions().
		SetLogger(logger).
		SetEnvironment(cfg.Environment).
		SetNamespace(cfg.Namespace), nil
}
