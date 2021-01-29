// Copyright (c) 2021 Uber Technologies, Inc.
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

package profiler

// Profiler represents profiler for profiling long-running tasks.
type Profiler interface {
	// StartCPUProfile starts the named cpu profile.
	StartCPUProfile(name string) error

	// StopCPUProfile stops started cpu profile.
	StopCPUProfile() error

	// WriteHeapProfile writes heap profile.
	WriteHeapProfile(name string) error
}

// Options represents the profiler options.
type Options interface {
	// Validate validates the options.
	Validate() error

	// Enabled returns if profile is enabled.
	Enabled() bool

	// SetEnabled enables profiling.
	SetEnabled(value bool) Options

	// Profiler returns the profiler.
	Profiler() Profiler

	// SetProfiler sets the profiler.
	SetProfiler(value Profiler) Options
}
