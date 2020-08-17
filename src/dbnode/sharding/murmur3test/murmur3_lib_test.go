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

package murmur3test

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	spaolaccimurmur3 "github.com/spaolacci/murmur3"
	twmbmurmur3 "github.com/twmb/murmur3"
)

func TestMurmurSum32(t *testing.T) {
	properties := gopter.NewProperties(newGopterTestParameters())
	properties.Property("twmb matches spaolacci", prop.ForAll(
		func(v []byte) bool {
			return twmbmurmur3.Sum32(v) == spaolaccimurmur3.Sum32(v)
		},
		newByteGen(),
	))

	properties.TestingRun(t)
}

func TestMurmurSum128(t *testing.T) {
	properties := gopter.NewProperties(newGopterTestParameters())
	properties.Property("twmb matches spaolacci", prop.ForAll(
		func(v []byte) bool {
			twmbH1, twmbH2 := twmbmurmur3.Sum128(v)
			spH1, spH2 := spaolaccimurmur3.Sum128(v)
			return spH1 == twmbH1 && spH2 == twmbH2
		},
		newByteGen(),
	))

	properties.TestingRun(t)
}

func TestMurmurSeedSum32(t *testing.T) {
	properties := gopter.NewProperties(newGopterTestParameters())
	properties.Property("twmb matches spaolacci", prop.ForAll(
		func(seed uint32, data []byte) bool {
			return twmbmurmur3.SeedSum32(seed, data) == spaolaccimurmur3.Sum32WithSeed(data, seed)
		},
		gen.UInt32(),
		newByteGen(),
	))

	properties.TestingRun(t)
}

func newGopterTestParameters() *gopter.TestParameters {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100000

	return params
}

// newByteGen returns a gopter generator for an []byte. All bytes (not just valid UTF-8) are generated.
func newByteGen() gopter.Gen {
	// uint8 == byte; therefore, derive []byte generator from []uint8 generator.
	return gopter.DeriveGen(func(v []uint8) []byte {
		out := make([]byte, len(v))
		for i, val := range v {
			out[i] = val
		}
		return out
	}, func(v []byte) []uint8 {
		out := make([]byte, len(v))
		for i, val := range v {
			out[i] = val
		}
		return out
	}, gen.SliceOf(gen.UInt8()))
}

