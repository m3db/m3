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
	"fmt"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	murmur3m3stack "github.com/m3db/stackmurmur3"
	murmur3spaolacci "github.com/spaolacci/murmur3"
	murmur3twmb "github.com/twmb/murmur3"
)

var (
	spaolacciaMurmur3 = murmur3Implementation{
		Name:   "spaolacci",
		Sum128: murmur3spaolacci.Sum128,
		Sum32:  murmur3spaolacci.Sum32,
		SeedSum32: func(seed uint32, data []byte) (h1 uint32) {
			return murmur3spaolacci.Sum32WithSeed(data, seed)
		},
	}

	twmbMurmur3 = murmur3Implementation{
		Name:   "twmb",
		Sum128: murmur3twmb.Sum128,
		Sum32:  murmur3twmb.Sum32,
		SeedSum32: murmur3twmb.SeedSum32,
	}

	m3stackMurmur3 = murmur3Implementation{
		Name:   "m3stack",
		Sum128: murmur3m3stack.Sum128,
		Sum32:  murmur3m3stack.Sum32,
		SeedSum32: func(seed uint32, data []byte) (h1 uint32) {
			return murmur3m3stack.Sum32WithSeed(data, seed)
		},
	}
)

func TestMurmurTwmbMatchesSpaolacci(t *testing.T) {
	testImplementationsMatch(t, spaolacciaMurmur3, twmbMurmur3)
}

func TestMurmurTwmbMatchesM3Stack(t *testing.T) {
	testImplementationsMatch(t, m3stackMurmur3, twmbMurmur3)
}

type murmur3Implementation struct{
	Name string
	Sum128 func(v []byte) (h1 uint64, h2 uint64)
	Sum32 func(v []byte) (h1 uint32)
	SeedSum32 func(seed uint32, data []byte) (h1 uint32)
}


func testImplementationsMatch(t *testing.T, old murmur3Implementation, new murmur3Implementation) {
	matchesTestName := fmt.Sprintf("%s matches %s", new.Name, old.Name)
	t.Run("Sum32", func(t *testing.T) {
		properties := gopter.NewProperties(newGopterTestParameters())
		properties.Property(matchesTestName, prop.ForAll(
			func(v []byte) bool {
				return new.Sum32(v) == old.Sum32(v)
			},
			newByteGen(),
		))

		properties.TestingRun(t)		
	})
	
	t.Run("Sum128", func(t *testing.T) {
		properties := gopter.NewProperties(newGopterTestParameters())
		properties.Property(matchesTestName, prop.ForAll(
			func(v []byte) bool {
				twmbH1, twmbH2 := new.Sum128(v)
				spH1, spH2 := old.Sum128(v)
				return spH1 == twmbH1 && spH2 == twmbH2
			},
			newByteGen(),
		))

		properties.TestingRun(t)
	})
	
	t.Run("SeedSum32", func(t *testing.T) {
		properties := gopter.NewProperties(newGopterTestParameters())
		properties.Property(matchesTestName, prop.ForAll(
			func(seed uint32, data []byte) bool {
				return new.SeedSum32(seed, data) == old.SeedSum32(seed, data)
			},
			gen.UInt32(),
			newByteGen(),
		))

		properties.TestingRun(t)
	})
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

