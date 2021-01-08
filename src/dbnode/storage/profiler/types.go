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

/*
 * // Copyright (c) 2021 Uber Technologies, Inc.
 * //
 * // Permission is hereby granted, free of charge, to any person obtaining a copy
 * // of this software and associated documentation files (the "Software"), to deal
 * // in the Software without restriction, including without limitation the rights
 * // to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * // copies of the Software, and to permit persons to whom the Software is
 * // furnished to do so, subject to the following conditions:
 * //
 * // The above copyright notice and this permission notice shall be included in
 * // all copies or substantial portions of the Software.
 * //
 * // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * // IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * // FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * // AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * // LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * // OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * // THE SOFTWARE.
 */

package profiler

// ProfileNamePrefix is the prefix of the profile name.
type ProfileNamePrefix string

const (
	// ProfileFileExtension is the extension of profile files.
	ProfileFileExtension = ".pb.gz"

	// BootstrapCPUProfileNamePrefix is prefix for bootstrap cpu profile filepath.
	BootstrapCPUProfileNamePrefix ProfileNamePrefix = "pprof.m3dbnode.samples.cpu."

	// BootstrapHeapProfileNamePrefix is prefix for bootstrap heap profile filepath.
	BootstrapHeapProfileNamePrefix ProfileNamePrefix = "pprof.m3dbnode.samples.heap."
)

// Options represents the profiler options.
type Options interface {
	// BootstrapProfileEnabled returns if bootstrap profile is enabled.
	BootstrapProfileEnabled() bool

	// SetBootstrapProfileEnabled enables bootstrap profiling.
	SetBootstrapProfileEnabled(value bool) Options

	// BootstrapProfilePath returns the bootstrapProfilePath.
	BootstrapProfilePath() string

	// SetBootstrapProfilePath sets the bootstrapProfilePath.
	SetBootstrapProfilePath(value string) Options
}
