// Copyright (c) 2018 Uber Technologies, Inc.
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

package composite

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	xtest "github.com/m3db/m3x/test"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func TestMergeBytesIterOverlappingProperty(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 1000
	parameters.MaxSize = 40
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	properties.Property("Merged overlapping iterators behave the same as the original inputs", prop.ForAll(
		func(inputs []string, numIters int) (bool, error) {
			inputs = deduplicate(inputs)
			original := newTestStringsIter(inputs)
			// split inputs into `numIters` iterators
			splits := make([][]string, numIters)
			for idx := 0; idx < numIters; idx++ {
				for i := 0; i < len(inputs); i++ {
					splits[idx] = append(splits[idx], inputs[i])
				}
			}
			splitIters := make([]sgmt.OrderedBytesIterator, 0, numIters)
			for _, split := range splits {
				splitIters = append(splitIters, newTestStringsIter(split))
			}
			// merge these split iterators into a single one
			mergeIter := newMergedOrderedBytesIterator(splitIters...)
			err := orderedBytesIteratorsEqual(original, mergeIter)
			return err == nil, err
		},
		gen.SliceOf(gen.AnyString()).WithLabel("inputs"),
		gen.IntRange(1, 10).WithLabel("numIters"),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func TestMergeBytesIterSplitProperty(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 1000
	parameters.MaxSize = 40
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	properties.Property("Merged Split iterators behave the same as the original inputs", prop.ForAll(
		func(inputs []string, numIters int) (bool, error) {
			inputs = deduplicate(inputs)
			original := newTestStringsIter(inputs)
			// split inputs into `numIters` iterators
			splits := make([][]string, numIters)
			for i := 0; i < len(inputs); i++ {
				idx := i % numIters
				splits[idx] = append(splits[idx], inputs[i])
			}
			splitIters := make([]sgmt.OrderedBytesIterator, 0, numIters)
			for _, split := range splits {
				splitIters = append(splitIters, newTestStringsIter(split))
			}
			// merge these split iterators into a single one
			mergeIter := newMergedOrderedBytesIterator(splitIters...)
			err := orderedBytesIteratorsEqual(original, mergeIter)
			return err == nil, err
		},
		gen.SliceOf(gen.AnyString()).WithLabel("inputs"),
		gen.IntRange(1, 10).WithLabel("numIters"),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func TestMergeBytesIterEmptyInputs(t *testing.T) {
	original := newTestStringsIter([]string{""})
	merged := newMergedOrderedBytesIterator(newTestStringsIter([]string{""}))
	require.NoError(t, orderedBytesIteratorsEqual(original, merged))
}

func TestMergeBytesIterTwoBackingIters(t *testing.T) {
	expected := newTestIter(
		testIterValue{str: "abc"},
		testIterValue{str: "bcd"},
		testIterValue{str: "cde"},
		testIterValue{str: "def"},
	)

	i1 := newTestIter(
		testIterValue{str: "abc"},
		testIterValue{str: "cde"},
	)
	i2 := newTestIter(
		testIterValue{str: "bcd"},
		testIterValue{str: "def"},
	)
	observed := newMergedOrderedBytesIterator(i1, i2)
	require.NoError(t, orderedBytesIteratorsEqual(expected, observed))
}

func TestMergeBytesIterTwoBackingOverlappingIters(t *testing.T) {
	expected := newTestIter(
		testIterValue{str: "abc"},
		testIterValue{str: "bcd"},
		testIterValue{str: "cde"},
		testIterValue{str: "def"},
	)

	i1 := newTestIter(
		testIterValue{str: "abc"},
		testIterValue{str: "bcd"},
		testIterValue{str: "cde"},
	)
	i2 := newTestIter(
		testIterValue{str: "bcd"},
		testIterValue{str: "cde"},
		testIterValue{str: "def"},
	)
	i3 := newTestIter(
		testIterValue{str: "abc"},
		testIterValue{str: "cde"},
		testIterValue{str: "def"},
	)
	observed := newMergedOrderedBytesIterator(i1, i2, i3)
	require.NoError(t, orderedBytesIteratorsEqual(expected, observed))
}

func TestMergeBytesIterSingleBackingIter(t *testing.T) {
	simpleTestCases(t, func(values ...testIterValue) sgmt.OrderedBytesIterator {
		return newMergedOrderedBytesIterator(newTestIter(values...))
	})
}

func TestMergeBytesIterStubIterSanityTest(t *testing.T) {
	simpleTestCases(t, newTestIter)
}

func simpleTestCases(t *testing.T, newIterFn newIterFn) {
	// empty value
	it := newIterFn()
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	// single value
	it = newIterFn(testIterValue{str: "abc"})
	require.True(t, it.Next())
	require.Equal(t, "abc", string(it.Current()))
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	// single value with error
	it = newIterFn(testIterValue{err: fmt.Errorf("abc")})
	require.False(t, it.Next())
	require.Error(t, it.Err())
	require.NoError(t, it.Close())

	// multiple value
	it = newIterFn(testIterValue{str: "abc"}, testIterValue{str: "def"})
	require.True(t, it.Next())
	require.Equal(t, "abc", string(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, "def", string(it.Current()))
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	// multiple value with error
	it = newIterFn(testIterValue{str: "abc"}, testIterValue{err: fmt.Errorf("def")})
	require.True(t, it.Next())
	require.Equal(t, "abc", string(it.Current()))
	require.False(t, it.Next())
	require.Error(t, it.Err())
	require.NoError(t, it.Close())
}

func orderedBytesIteratorsEqual(a, b sgmt.OrderedBytesIterator) error {
	// see if both a & b have more elements
	aNext := a.Next()
	if aNext != b.Next() {
		if aNext {
			return fmt.Errorf("a has more elements than b")
		}
		return fmt.Errorf("b has more elements than a")
	}

	// if they do, ensure the elements are equal & recurse
	if aNext {
		if !bytes.Equal(a.Current(), b.Current()) {
			return fmt.Errorf("values are un-equal: %v, %v",
				string(a.Current()), string(b.Current()))
		}
		// recurse
		return orderedBytesIteratorsEqual(a, b)
	}

	// base-case - i.e. both a & b have no further elements
	if a.Err() != b.Err() {
		return fmt.Errorf("errors are un-equal: %v %v", a.Err(), b.Err())
	}

	aClose := a.Close()
	bClose := b.Close()
	if aClose != bClose {
		return fmt.Errorf("closes are un-equal: %v %v", aClose, bClose)
	}

	return nil
}

type newIterFn func(values ...testIterValue) sgmt.OrderedBytesIterator

func TestDeduplicate(t *testing.T) {
	a := []string{"a", "b", "c"}
	b := []string{"a", "a", "b", "c", "c"}
	b = deduplicate(b)
	require.True(t, xtest.CmpMatcher(a).Matches(b))
	require.True(t, xtest.CmpMatcher(b).Matches(a))
}

func deduplicate(vals []string) []string {
	sort.Slice(vals, func(i, j int) bool {
		return strings.Compare(vals[i], vals[j]) < 0
	})
	idx := 0
	last := ""
	for idx < len(vals) {
		current := vals[idx]
		if idx != 0 && last == current {
			vals = append(vals[:idx], vals[idx+1:]...)
			continue
		}
		last = current
		idx++
	}
	return vals
}

func newTestStringsIter(values []string) sgmt.OrderedBytesIterator {
	vals := make([]testIterValue, 0, len(values))
	for _, val := range values {
		vals = append(vals, testIterValue{str: val})
	}
	sort.Slice(vals, func(i, j int) bool {
		return strings.Compare(vals[i].str, vals[j].str) < 0
	})
	return newTestIter(vals...)
}

func newTestIter(values ...testIterValue) sgmt.OrderedBytesIterator {
	return &stubIter{
		idx:    -1,
		values: values,
	}
}

type testIterValue struct {
	str string
	err error
}

type stubIter struct {
	closed bool
	err    error
	idx    int
	values []testIterValue
}

func (s *stubIter) Next() bool {
	if s.err != nil || s.closed {
		return false
	}
	s.idx++
	if s.idx >= len(s.values) {
		return false
	}
	if err := s.values[s.idx].err; err != nil {
		s.err = err
		return false
	}
	return true
}

func (s *stubIter) Current() []byte {
	return []byte(s.values[s.idx].str)
}

func (s *stubIter) Err() error {
	return s.err
}

func (s *stubIter) Close() error {
	if s.closed {
		return errIterAlreadyClosed
	}
	s.closed = true
	return nil
}

func (s *stubIter) Len() int {
	return len(s.values)
}

func toSlice(t *testing.T, iter sgmt.OrderedBytesIterator) [][]byte {
	elems := [][]byte{}
	for iter.Next() {
		curr := iter.Current()
		bytes := append([]byte(nil), curr...)
		elems = append(elems, bytes)
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return elems
}
