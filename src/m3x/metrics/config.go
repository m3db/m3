// Copyright (c) 2016 Uber Technologies, Inc.
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

package xmetrics

import (
	"github.com/cactus/go-statsd-client/statsd"
)

// Configuration defines configuration for sending to statsd
type Configuration struct {
	ServerAddress string `json:"serverAddress",yaml:"serverAddress"`
	Prefix        string `json:"prefix",yaml:"prefix"`
}

// BuildStatsReporter builds a new StatsReporter from the configuration
func (cfg Configuration) BuildStatsReporter() (StatsReporter, error) {
	if len(cfg.ServerAddress) == 0 {
		return NullStatsReporter, nil
	}

	statsd, err := statsd.New(cfg.ServerAddress, cfg.Prefix)
	if err != nil {
		return nil, err
	}

	return NewCactusStatsReporter(statsd), nil
}
