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

import "sync/atomic"

// ProfileNamePrefix is the prefix of the profile name.
type ProfileNamePrefix struct {
	name  string
	count int32
}

func (p *ProfileNamePrefix) inc() int32 {
	return atomic.AddInt32(&p.count, 1)
}

// ServiceName is the gcp service name.
type ServiceName string

// String returns service name string.
func (s ServiceName) String() string {
	return string(s)
}

var (
	// PeersBootstrapReadDataCPUProfileNamePrefix is prefix for peers bootstrap read data cpu profile filepath.
	PeersBootstrapReadDataCPUProfileNamePrefix = &ProfileNamePrefix{
		name: "pprof.m3dbnode.peers.data.samples.cpu.",
	}

	// PeersBootstrapReadIndexCPUProfileNamePrefix is prefix for peers bootstrap read index cpu profile filepath.
	PeersBootstrapReadIndexCPUProfileNamePrefix = &ProfileNamePrefix{
		name: "pprof.m3dbnode.peers.index.samples.cpu.",
	}

	// PeersBootstrapReadDataHeapProfileNamePrefix is prefix for peers bootstrap read data heap profile filepath.
	PeersBootstrapReadDataHeapProfileNamePrefix = &ProfileNamePrefix{
		name: "pprof.m3dbnode.peers.data.samples.heap.",
	}
)

const (
	// ProfileFileExtension is the extension of profile files.
	ProfileFileExtension = ".pb.gz"

	// ServiceNamePeersBootstrapReadData is service name for gcp cloud profiler.
	ServiceNamePeersBootstrapReadData ServiceName = "m3dbnode-peer-bootstrap-read-data"

	// ServiceNamePeersBootstrapReadIndex is service name for gcp cloud profiler.
	ServiceNamePeersBootstrapReadIndex ServiceName = "m3dbnode-peer-bootstrap-read-index"
)

// Options represents the profiler options.
type Options interface {
	// Enabled returns if profile is enabled.
	Enabled() bool

	// SetEnabled enables profiling.
	SetEnabled(value bool) Options

	// ProfilePath returns the profile path.
	ProfilePath() string

	// SetProfilePath sets the profile path.
	SetProfilePath(value string) Options
}
