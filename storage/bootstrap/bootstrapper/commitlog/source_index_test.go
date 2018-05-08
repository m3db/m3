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

package commitlog

import (
	"testing"
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestBootstrapIndex(t *testing.T) {
	var (
		opts             = testOptions()
		src              = newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)
		dataBlockSize    = 2 * time.Hour
		indexBlockSize   = 4 * time.Hour
		namespaceOptions = namespace.NewOptions().
					SetRetentionOptions(
				namespace.NewOptions().
					RetentionOptions().
					SetBlockSize(dataBlockSize),
			).
			SetIndexOptions(
				namespace.NewOptions().
					IndexOptions().
					SetBlockSize(indexBlockSize),
			)
	)
	md, err := namespace.NewMetadata(testNamespaceID, namespaceOptions)
	require.NoError(t, err)

	now := time.Now()
	start := now.Truncate(dataBlockSize).Add(-2 * dataBlockSize)

	fooTags := ident.Tags{ident.StringTag("city", "ny"), ident.StringTag("conference", "monitoroma")}
	barTags := ident.Tags{ident.StringTag("city", "sf")}
	bazTags := ident.Tags{ident.StringTag("city", "oakland")}
	foo := commitlog.Series{Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("foo"), Tags: fooTags}
	bar := commitlog.Series{Namespace: testNamespaceID, Shard: 1, ID: ident.StringID("bar"), Tags: barTags}
	baz := commitlog.Series{Namespace: testNamespaceID, Shard: 2, ID: ident.StringID("baz"), Tags: bazTags}

	values := []testValue{
		{foo, start, 1.0, xtime.Second, nil},
		{foo, start, 2.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 2.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 2.0, xtime.Second, nil},
	}

	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	ranges := xtime.Ranges{}
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(-2 * dataBlockSize),
		End:   start.Add(-dataBlockSize),
	})
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(-dataBlockSize),
		End:   start,
	})
	ranges = ranges.AddRange(xtime.Range{
		Start: start,
		End:   start.Add(dataBlockSize),
	})

	targetRanges := result.ShardTimeRanges{0: ranges, 1: ranges, 2: ranges}
	res, err := src.ReadIndex(md, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)

	// Data blockSize is 2 hours and index blockSize is four hours so the data blocks
	// will span two different index blocks.
	require.Equal(t, 2, len(res.IndexResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
}
