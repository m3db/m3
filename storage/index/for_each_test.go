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

package index

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3ninx/doc"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestForEachBlockStarts(t *testing.T) {
	now := time.Now().Truncate(time.Hour)
	tn := func(n int64) xtime.UnixNano {
		return xtime.ToUnixNano(now.Add(time.Duration(n) * time.Hour))
	}
	d := func(n int) doc.Document {
		return doc.Document{
			ID: []byte(fmt.Sprintf("%d", n)),
		}
	}
	entries := WriteBatchEntryByBlockStart([]WriteBatchEntry{
		WriteBatchEntry{BlockStart: tn(2), Document: d(2)},
		WriteBatchEntry{BlockStart: tn(0), Document: d(0)},
		WriteBatchEntry{BlockStart: tn(1), Document: d(1)},
	})
	sort.Sort(entries)

	numCalls := 0
	entries.ForEachBlockStart(func(ts xtime.UnixNano, writes []WriteBatchEntry) {
		require.Equal(t, 1, len(writes))
		switch numCalls {
		case 0:
			require.Equal(t, "0", string(writes[0].Document.ID))
		case 1:
			require.Equal(t, "1", string(writes[0].Document.ID))
		case 2:
			require.Equal(t, "2", string(writes[0].Document.ID))
		default:
			require.FailNow(t, "should never get here")
		}
		numCalls++
	})
	require.Equal(t, 3, numCalls)
}

func TestForEachBlockStartMore(t *testing.T) {
	now := time.Now().Truncate(time.Hour)
	tn := func(n int64) xtime.UnixNano {
		return xtime.ToUnixNano(now.Add(time.Duration(n) * time.Hour))
	}
	d := func(n int) doc.Document {
		return doc.Document{
			ID: []byte(fmt.Sprintf("%d", n)),
		}
	}
	entries := WriteBatchEntryByBlockStart([]WriteBatchEntry{
		WriteBatchEntry{BlockStart: tn(0), Document: d(0)},
		WriteBatchEntry{BlockStart: tn(1), Document: d(3)},
		WriteBatchEntry{BlockStart: tn(0), Document: d(1)},
		WriteBatchEntry{BlockStart: tn(1), Document: d(4)},
		WriteBatchEntry{BlockStart: tn(0), Document: d(2)},
	})
	sort.Sort(entries)

	numCalls := 0
	entries.ForEachBlockStart(func(ts xtime.UnixNano, writes []WriteBatchEntry) {
		switch numCalls {
		case 0:
			require.Equal(t, 3, len(writes))
			require.Equal(t, "0", string(writes[0].Document.ID))
			require.Equal(t, "1", string(writes[1].Document.ID))
			require.Equal(t, "2", string(writes[2].Document.ID))
		case 1:
			require.Equal(t, 2, len(writes))
			require.Equal(t, "3", string(writes[0].Document.ID))
			require.Equal(t, "4", string(writes[1].Document.ID))
		default:
			require.FailNow(t, "should never get here")
		}
		numCalls++
	})
	require.Equal(t, 2, numCalls)
}
