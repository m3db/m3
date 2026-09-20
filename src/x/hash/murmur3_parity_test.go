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
	"math/rand"
	"testing"

	stackmurmur3 "github.com/m3db/stackmurmur3/v2"
	"github.com/stretchr/testify/require"
	"github.com/twmb/murmur3"
)

// murmur3 outputs feed shard assignment (dbnode/sharding, aggregator/sharding)
// and rule matching, so the two implementations in the dependency graph must
// stay bit-identical. This guards the swap from stackmurmur3 to twmb/murmur3
// and pins the outputs to published MurmurHash3 reference vectors so neither
// library can silently drift.
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

		require.Equal(t, v.h32, stackmurmur3.Sum32(b), "stackmurmur3.Sum32(%q)", v.s)
		require.Equal(t, v.h32, murmur3.Sum32(b), "twmb.Sum32(%q)", v.s)
		require.Equal(t, v.h32, stackmurmur3.StringSum32(v.s), "stackmurmur3.StringSum32(%q)", v.s)
		require.Equal(t, v.h32, murmur3.StringSum32(v.s), "twmb.StringSum32(%q)", v.s)
		require.Equal(t, v.h32, stackmurmur3.SeedSum32(0, b), "stackmurmur3.SeedSum32(0, %q)", v.s)
		require.Equal(t, v.h32, murmur3.SeedSum32(0, b), "twmb.SeedSum32(0, %q)", v.s)

		require.Equal(t, v.h64_1, stackmurmur3.Sum64(b), "stackmurmur3.Sum64(%q)", v.s)
		require.Equal(t, v.h64_1, murmur3.Sum64(b), "twmb.Sum64(%q)", v.s)

		s1, s2 := stackmurmur3.Sum128(b)
		require.Equal(t, v.h64_1, s1, "stackmurmur3.Sum128(%q) h1", v.s)
		require.Equal(t, v.h64_2, s2, "stackmurmur3.Sum128(%q) h2", v.s)
		t1, t2 := murmur3.Sum128(b)
		require.Equal(t, v.h64_1, t1, "twmb.Sum128(%q) h1", v.s)
		require.Equal(t, v.h64_2, t2, "twmb.Sum128(%q) h2", v.s)
	}
}

func TestMurmur3Parity(t *testing.T) {
	// Fixed seed so a failure is reproducible.
	rng := rand.New(rand.NewSource(42))

	seeds := []uint32{0, 1, 0x7fffffff, 0x80000000, 0xffffffff, rng.Uint32(), rng.Uint32()}

	var inputs [][]byte
	// Every length up to 64 exercises all tail-byte paths of the 32-bit (4-byte
	// block) and 64/128-bit (16-byte block) variants several times over.
	for n := 0; n <= 64; n++ {
		b := make([]byte, n)
		rng.Read(b)
		inputs = append(inputs, b)
	}
	// Longer inputs of random size, including ones spanning many blocks.
	for i := 0; i < 256; i++ {
		b := make([]byte, 65+rng.Intn(4096))
		rng.Read(b)
		inputs = append(inputs, b)
	}
	// Byte patterns that catch sign-extension and endianness mistakes.
	for _, fill := range []byte{0x00, 0x7f, 0x80, 0xff} {
		b := make([]byte, 1024)
		for i := range b {
			b[i] = fill
		}
		inputs = append(inputs, b)
	}

	for i, b := range inputs {
		require.Equal(t, stackmurmur3.Sum32(b), murmur3.Sum32(b), "Sum32 input %d len %d", i, len(b))
		require.Equal(t, stackmurmur3.Sum64(b), murmur3.Sum64(b), "Sum64 input %d len %d", i, len(b))

		s1, s2 := stackmurmur3.Sum128(b)
		t1, t2 := murmur3.Sum128(b)
		require.Equal(t, s1, t1, "Sum128 h1 input %d len %d", i, len(b))
		require.Equal(t, s2, t2, "Sum128 h2 input %d len %d", i, len(b))

		s := string(b)
		require.Equal(t, stackmurmur3.StringSum32(s), murmur3.StringSum32(s), "StringSum32 input %d len %d", i, len(b))

		for _, seed := range seeds {
			require.Equal(t, stackmurmur3.SeedSum32(seed, b), murmur3.SeedSum32(seed, b),
				"SeedSum32 seed %#x input %d len %d", seed, i, len(b))
		}
	}
}
