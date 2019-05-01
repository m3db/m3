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
	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/log"
)

// Configuration contains top-level configuration.
type Configuration struct {
	// Logging configuration.
	Logging log.Configuration `yaml:"logging"`

	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// Raw TCP server configuration.
	RawTCP RawTCPServerConfiguration `yaml:"rawtcp"`

	// HTTP server configuration.
	HTTP HTTPServerConfiguration `yaml:"http"`

	// M3msg server configuration.
	M3msg *m3msg.Configuration `yaml:"m3msg"`

	// Client configuration for key value store.
	KVClient KVClientConfiguration `yaml:"kvClient" validate:"nonzero"`

	// Runtime options configuration.
	RuntimeOptions RuntimeOptionsConfiguration `yaml:"runtimeOptions"`

	// Aggregator configuration.
	Aggregator AggregatorConfiguration `yaml:"aggregator"`
}
