// Copyright (c) 2016 Uber Technologies, Inc.
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

package time

import (
	"container/list"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	testTimeRanges = []Range{
		{Start: testStart, End: testStart.Add(time.Second)},
		{Start: testStart.Add(2 * time.Second), End: testStart.Add(10 * time.Second)},
		{Start: testStart.Add(20 * time.Second), End: testStart.Add(25 * time.Second)},
	}
)

func getTestList() *list.List {
	l := list.New()
	for _, r := range testTimeRanges {
		l.PushBack(r)
	}
	return l
}

func TestRangeIter(t *testing.T) {
	it := newRangeIter(nil)
	require.False(t, it.Next())

	it = newRangeIter(getTestList())
	for i := 0; i < len(testTimeRanges); i++ {
		require.True(t, it.Next())
		require.Equal(t, testTimeRanges[i], it.Value())
	}
	require.False(t, it.Next())

}
