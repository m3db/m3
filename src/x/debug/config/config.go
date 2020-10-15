// Copyright (c) 2020 Uber Technologies, Inc.
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
	"runtime"

	"go.uber.org/zap"
)

// DebugConfiguration for the debug package.
type DebugConfiguration struct {

	// MutexProfileFraction is used to set the runtime.SetMutexProfileFraction to report mutex contention events.
	// See https://tip.golang.org/pkg/runtime/#SetMutexProfileFraction for more details about the value.
	MutexProfileFraction int `yaml:"mutexProfileFraction"`

	// BlockProfileRate is used to set the runtime.BlockProfileRate to report blocking events
	// See https://golang.org/pkg/runtime/#SetBlockProfileRate for more details about the value.
	BlockProfileRate int `yaml:"blockProfileRate"`
}

// SetRuntimeValues sets the configured pprof runtime values.
func (c DebugConfiguration) SetRuntimeValues(logger *zap.Logger) {
	logger.Info(fmt.Sprintf("setting MutexProfileFraction: %v", c.MutexProfileFraction))
	runtime.SetMutexProfileFraction(c.MutexProfileFraction)
	logger.Info(fmt.Sprintf("setting BlockProfileRate: %v", c.BlockProfileRate))
	runtime.SetBlockProfileRate(c.BlockProfileRate)
}
