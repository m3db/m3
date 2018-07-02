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
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/instrument"
)

// Configuration represents the knobs available to configure a m3nsch_server
type Configuration struct {
	Server  ServerConfiguration             `yaml:"server"`
	M3nsch  M3nschConfiguration             `yaml:"m3nsch" validate:"nonzero"`
	Metrics instrument.MetricsConfiguration `yaml:"metrics" validate:"nonzero"`
}

// ServerConfiguration represents the knobs available to configure server properties
type ServerConfiguration struct {
	ListenAddress string  `yaml:"listenAddress" validate:"nonzero"`
	DebugAddress  string  `yaml:"debugAddress" validate:"nonzero"`
	CPUFactor     float64 `yaml:"cpuFactor" validate:"min=0.5,max=3.0"`
}

// M3nschConfiguration represents the knobs available to configure m3nsch properties
type M3nschConfiguration struct {
	Concurrency       int `yaml:"concurrency" validate:"min=500,max=5000"`
	NumPointsPerDatum int `yaml:"numPointsPerDatum" validate:"min=10,max=120"`
}

// New returns a Configuration read from the specified path
func New(filename string) (Configuration, error) {
	var conf Configuration
	err := xconfig.LoadFile(&conf, filename, xconfig.Options{})
	return conf, err
}
