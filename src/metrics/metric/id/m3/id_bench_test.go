// Copyright (c) 2024 Uber Technologies, Inc.
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

package m3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Results 2024-10-17:
// goos: darwin
// goarch: arm64
// pkg: github.com/m3db/m3/src/metrics/metric/id/m3
// BenchmarkIsRollupID
// BenchmarkIsRollupID-10    	     349	   3404269 ns/op	       0 B/op	       0 allocs/op
// PASS

func BenchmarkIsRollupID(b *testing.B) {
	id := []byte("m3+foo+m3_rollup=true,tagName1=tagValue1")

	name, tags, err := NameAndTags(id)
	require.NoError(b, err)

	assert.Equal(b, []byte("foo"), name)
	assert.Equal(b, []byte("m3_rollup=true,tagName1=tagValue1"), tags)
	assert.True(b, IsRollupID(name, tags))

	b.ReportAllocs()
	b.ResetTimer()

	loopResult := false
	for i := 0; i < b.N; i++ {
		result := false
		for j := 0; j < 100_000; j++ {
			result = IsRollupID(name, tags)
		}
		loopResult = result
	}

	require.True(b, loopResult)
}

// Test_IsRollupID_doesntAlloc is a sanity check that IsRollupID doesn't require any iterator allocations.
// If you hit a failure on this assertion, it *could* be okay to change it to allow for some allocations.
// However, this is a fairly performance sensitive path--think carefully about what you're doing.
// It's also possible that a Go version upgrade will change this; consider whether it makes sense to tweak
// the approach at that time.
func Test_IsRollupID_doesntAlloc(t *testing.T) {
	id := []byte("m3+foo+m3_rollup=true,tagName1=tagValue1")

	name, tags, err := NameAndTags(id)
	require.NoError(t, err)

	allocated := testing.AllocsPerRun(10, func() {
		IsRollupID(name, tags)
	})

	assert.Equal(t, 0.0, allocated, "IsRollupID should not require allocating memory")
}
