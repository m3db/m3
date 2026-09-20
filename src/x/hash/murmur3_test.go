// Copyright (c) 2026 Uber Technologies, Inc.
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

package hash

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/murmur3"
)

// murmur3 outputs feed shard assignment (dbnode/sharding, aggregator/sharding)
// and rule matching, so the implementation must stay bit-identical to the
// published MurmurHash3 reference vectors: any drift would silently move data
// between shards.
var murmur3ReferenceVectors = []struct {
	h32   uint32
	h64_1 uint64
	h64_2 uint64
	s     string
}{
	{0x00000000, 0x0000000000000000, 0x0000000000000000, ""},
	{0x248bfa47, 0xcbd8a7b341bd9b02, 0x5b1e906a48ae1d19, "hello"},
	{0x149bbb7f, 0x342fac623a5ebc8e, 0x4cdcbc079642414d, "hello, world"},
	{0xe31e8a70, 0xb89e5988b737affc, 0x664fc2950231b2cb, "19 Jan 2038 at 3:14:07 AM"},
	{0xd5c48bfc, 0xcd99481f9ee902c9, 0x695da1a38987b6e7, "The quick brown fox jumps over the lazy dog."},
}

func TestMurmur3ReferenceVectors(t *testing.T) {
	for _, v := range murmur3ReferenceVectors {
		b := []byte(v.s)

		require.Equal(t, v.h32, murmur3.Sum32(b), "Sum32(%q)", v.s)
		require.Equal(t, v.h32, murmur3.StringSum32(v.s), "StringSum32(%q)", v.s)
		require.Equal(t, v.h32, murmur3.SeedSum32(0, b), "SeedSum32(0, %q)", v.s)
		require.Equal(t, v.h64_1, murmur3.Sum64(b), "Sum64(%q)", v.s)

		h1, h2 := murmur3.Sum128(b)
		require.Equal(t, v.h64_1, h1, "Sum128(%q) h1", v.s)
		require.Equal(t, v.h64_2, h2, "Sum128(%q) h2", v.s)
	}
}
