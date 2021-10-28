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

package pools

import (
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/encoding"
)

func TestBuildIteratorPoolsHasSaneDefaults(t *testing.T) {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	// TotalAlloc increases as heap objects are allocated, but
	// unlike Alloc and HeapAlloc, it does not decrease when
	// objects are freed.
	totalAllocBefore := stats.TotalAlloc

	BuildIteratorPools(encoding.NewOptions(), BuildIteratorPoolsOptions{})

	runtime.ReadMemStats(&stats)

	allocated := int(stats.TotalAlloc - totalAllocBefore)
	t.Logf("allocated %v bytes", allocated)

	upperLimit := 64 * 1024 * 1024 // 64mb
	require.True(t, allocated < upperLimit,
		"allocated more than "+strconv.Itoa(upperLimit)+" bytes")
}
