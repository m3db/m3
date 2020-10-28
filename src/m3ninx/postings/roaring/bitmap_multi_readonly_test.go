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

package roaring

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/m3dbx/pilosa/roaring"
	"github.com/stretchr/testify/require"
)

func TestMultiBitmap(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	each := 8

	type testCasePostingsCombinators struct {
		numRegular     int
		numUnion       int
		numNegate      int
		numNegateUnion int
	}

	type testCasePostingsDistribution struct {
		insertCount int
		insertRange int
	}

	type testCase struct {
		testCasePostingsCombinators
		testCasePostingsDistribution
	}

	combinators := []testCasePostingsCombinators{
		{
			numRegular:     2,
			numUnion:       0,
			numNegate:      0,
			numNegateUnion: 0,
		},
		{
			numRegular:     2,
			numUnion:       2,
			numNegate:      0,
			numNegateUnion: 0,
		},
		{
			numRegular:     2,
			numUnion:       2,
			numNegate:      1,
			numNegateUnion: 0,
		},
		{
			numRegular:     2,
			numUnion:       2,
			numNegate:      1,
			numNegateUnion: 2,
		},
	}

	distributions := []struct {
		insertCount int
		insertRange int
	}{
		// 64 inserts
		{
			insertCount: 64,
			insertRange: 64,
		},
		{
			insertCount: 64,
			insertRange: 128,
		},
		{
			insertCount: 64,
			insertRange: 256,
		},
		// 4096 inserts
		{
			insertCount: 4096,
			insertRange: 4096,
		},
		{
			insertCount: 4096,
			insertRange: 8192,
		},
		{
			insertCount: 4096,
			insertRange: 16384,
		},
		// 65536 inserts
		{
			insertCount: 65536,
			insertRange: 65536,
		},
		{
			insertCount: 65536,
			insertRange: 131072,
		},
		{
			insertCount: 65536,
			insertRange: 262144,
		},
	}

	var tests []testCase
	for _, c := range combinators {
		for _, d := range distributions {
			tests = append(tests, testCase{
				testCasePostingsCombinators:  c,
				testCasePostingsDistribution: d,
			})
		}
	}

	for _, test := range tests {
		genOpts := genRandBitmapAndReadOnlyBitmapOptions{
			rng:         rng,
			insertRange: test.insertRange,
			insertCount: test.insertCount,
		}
		for i := 0; i < each; i++ {
			t.Run(fmt.Sprintf("attempt=%d, test=+%v", i, test), func(t *testing.T) {
				allReadOnly, err := NewReadOnlyBitmapRange(0, uint64(test.insertRange))
				require.NoError(t, err)

				reg, regReadOnly :=
					genRandBitmapsAndReadOnlyBitmaps(t, test.numRegular, genOpts)
				union, unionReadOnly :=
					genRandBitmapsAndReadOnlyBitmaps(t, test.numUnion, genOpts)
				negate, negateReadOnly :=
					genRandBitmapsAndReadOnlyBitmaps(t, test.numNegate, genOpts)
				negateUnion, negateUnionReadOnly :=
					genRandBitmapsAndReadOnlyBitmaps(t, test.numNegateUnion, genOpts)

				// First create the inner multi-bitmaps.
				multiInner := concat(regReadOnly)

				if test.numUnion > 0 {
					innerUnion, err := UnionReadOnly(unionReadOnly)
					require.NoError(t, err)
					multiInner = append(multiInner, innerUnion)
				}

				if test.numNegate > 0 {
					innerNegate, err := IntersectAndNegateReadOnly(lists(allReadOnly), negateReadOnly)
					require.NoError(t, err)
					multiInner = append(multiInner, innerNegate)
				}

				if test.numNegateUnion > 0 {
					innerNegateUnionUnion, err := UnionReadOnly(negateUnionReadOnly)
					require.NoError(t, err)
					innerNegateUnion, err := IntersectAndNegateReadOnly(lists(allReadOnly), lists(innerNegateUnionUnion))
					require.NoError(t, err)
					multiInner = append(multiInner, innerNegateUnion)
				}

				// Create top level multi-bitmap.
				multi, err := IntersectAndNegateReadOnly(multiInner, nil)
				require.NoError(t, err)

				// Perform same operations the old way with postings lists.
				bitmap := roaring.NewBitmap()
				// Make sure at least some regular postings lists are being
				// intersected, otherwise starting with all bitmap won't be
				// useful.
				require.True(t, len(reg) > 0)
				// First set all bits in the range.
				bitmap = bitmap.Flip(0, uint64(test.insertRange))
				// Intersect with regular bitmaps now.
				for _, pl := range reg {
					bitmap = bitmap.Intersect(bitmapFromPostings(t, pl))
				}
				// Intersect with union.
				if test.numUnion > 0 {
					pl, err := Union(union)
					require.NoError(t, err)
					bitmap = bitmap.Intersect(bitmapFromPostings(t, pl))
				}
				// Intersect with negate.
				if test.numNegate > 0 {
					// Create top level multi-bitmap.
					for _, pl := range negate {
						bitmap = bitmap.Difference(bitmapFromPostings(t, pl))
					}
				}
				// Intersect with negate of union.
				if test.numNegateUnion > 0 {
					pl, err := Union(negateUnion)
					require.NoError(t, err)
					bitmap = bitmap.Difference(bitmapFromPostings(t, pl))
				}
				transformed := NewPostingsListFromBitmap(bitmap)

				// Check for equality.
				equal := postings.Equal(multi, transformed)
				if !equal {
					fmt.Printf("negate: %v\n", postingsString(negate[0]))
					msg := fmt.Sprintf("multi-bitmap: %s\nstandard: %s\n",
						postingsString(multi), postingsString(transformed))

					if debug := os.Getenv("TEST_DEBUG_DIR"); debug != "" {
						e0 := ioutil.WriteFile(path.Join(debug, "actual.json"), []byte(postingsJSON(t, multi)), 0666)
						e1 := ioutil.WriteFile(path.Join(debug, "expected.json"), []byte(postingsJSON(t, transformed)), 0666)
						require.NoError(t, e0)
						require.NoError(t, e1)
						msg += fmt.Sprintf("wrote debug: %s\n", debug)
					}
					require.True(t, equal, msg)
				}

				// Check for contains.
				// iter := transformed.Iterator()
				// for iter.Next() {
				// 	curr := iter.Current()
				// 	require.True(t, multi.Contains(curr))
				// }
				// require.NoError(t, iter.Err())
				// require.NoError(t, iter.Close())
			})
		}
	}
}

func bitmapFromPostings(t *testing.T, pl postings.List) *roaring.Bitmap {
	b, ok := BitmapFromPostingsList(pl)
	require.True(t, ok)
	return b
}

func lists(list ...postings.List) []postings.List {
	return list
}

func concat(lists ...[]postings.List) []postings.List {
	var result []postings.List
	for _, list := range lists {
		result = append(result, list...)
	}
	return result
}

func genRandBitmapsAndReadOnlyBitmaps(
	t *testing.T,
	count int,
	opts genRandBitmapAndReadOnlyBitmapOptions,
) ([]postings.List, []postings.List) {
	var regular, readOnlys []postings.List
	for i := 0; i < count; i++ {
		list, readOnly := genRandBitmapAndReadOnlyBitmap(t, opts)
		regular = append(regular, list)
		readOnlys = append(readOnlys, readOnly)
	}
	return regular, readOnlys
}

type genRandBitmapAndReadOnlyBitmapOptions struct {
	rng         *rand.Rand
	insertRange int
	insertCount int
}

func genRandBitmapAndReadOnlyBitmap(
	t *testing.T,
	opts genRandBitmapAndReadOnlyBitmapOptions,
) (postings.List, *ReadOnlyBitmap) {
	// Note: do not reuse bitmap since we return postings list which
	// references it.
	bitmap := roaring.NewBitmap()

	// Guarantee at least one.
	max := uint64(opts.rng.Int63n(int64(opts.insertRange-1))) + 1
	for j := 0; j < opts.insertCount; j++ {
		value := opts.rng.Uint64() % max
		bitmap.DirectAdd(value)
	}

	list := NewPostingsListFromBitmap(bitmap)

	// Note: do not reuse buffer since read only bitmap
	// references them.
	buff := bytes.NewBuffer(nil)
	_, err := bitmap.WriteTo(buff)
	require.NoError(t, err)

	readOnly, err := NewReadOnlyBitmap(buff.Bytes())
	require.NoError(t, err)

	return list, readOnly
}

func postingsString(pl postings.List) string {
	var buf bytes.Buffer
	iter := pl.Iterator()
	for i := 0; iter.Next(); i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%d", iter.Current()))
	}
	return "[" + buf.String() + "]"
}

func postingsJSON(t *testing.T, pl postings.List) string {
	var out []uint64
	iter := pl.Iterator()
	for i := 0; iter.Next(); i++ {
		out = append(out, uint64(iter.Current()))
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	data, err := json.MarshalIndent(out, "", "  ")
	require.NoError(t, err)
	return string(data)
}
