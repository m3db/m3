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

package wide

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildTestBuffer(bls ...ident.IndexChecksumBlock) IndexChecksumBlockBuffer {
	buffer := NewIndexChecksumBlockBuffer()
	go func() {
		for _, bl := range bls {
			buffer.Push(bl)
		}
		buffer.Close()
	}()
	return buffer
}

// buildOpts builds default test options. Notable here is the xtest.NewHash32()
// implementation of the hash.Hash32 interface; this test hash function sets
// hash value for an entry as any string represented integer values in the
// entry ID + entry tags.
func buildOpts(t *testing.T) Options {
	opts := NewOptions().
		SetBatchSize(2).
		SetBytesPool(testBytesPool()).
		SetDecodingOptions(msgpack.NewDecodingOptions().SetHash32(xtest.NewHash32(t))).
		SetInstrumentOptions(instrument.NewOptions())
	require.NoError(t, opts.Validate())
	return opts
}

func idxEntry(id, tags string) schema.IndexEntry {
	return schema.IndexEntry{
		ID:          []byte(id),
		EncodedTags: []byte(tags),
	}
}

func toEntry(id, tags string, checksum int64) entry {
	return entry{
		entry:      idxEntry(id, tags),
		idChecksum: checksum,
	}
}

func testIdxMismatch(checksum int64) ReadMismatch {
	return ReadMismatch{
		Mismatched: true,
		Checksum:   checksum,
	}
}

func testEntryMismatch(id, tags string, checksum int64) ReadMismatch {
	return ReadMismatch{
		Mismatched:  true,
		Checksum:    checksum,
		ID:          []byte(id),
		EncodedTags: []byte(tags),
	}
}

func testMismatches(t *testing.T, expected, actual []ReadMismatch) {
	require.Equal(t, len(expected), len(actual))
	for i, ex := range expected {
		assert.True(t, ex.Equal(actual[i]), fmt.Sprintf(
			"match failed at idx %d\n expected : %+v\n actual   : %+v",
			i, ex, actual[i]))
	}
}

func TestEmitMismatches(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	bp := pool.NewMockBytesPool(ctrl)
	id, tags := "foo", "encoded-tags"
	id1, tags1 := "foo1", "encoded-tags1"
	id2, tags2 := "foo2", "encoded-tags2"
	gomock.InOrder(
		bp.EXPECT().Put([]byte(id)),
		bp.EXPECT().Put([]byte(tags)),
		bp.EXPECT().Put([]byte(id1)),
		bp.EXPECT().Put([]byte(tags1)),
		bp.EXPECT().Put([]byte(id2)),
		bp.EXPECT().Put([]byte(tags2)),
	)

	buffer := buildTestBuffer()
	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t).SetBytesPool(bp))
	checker, ok := chk.(*entryChecksumMismatchChecker)
	require.True(t, ok)

	checker.entryMismatches(toEntry(id, tags, 0), toEntry(id1, tags1, 1))
	checker.indexMismatches(100, 200)
	checker.entryMismatches(toEntry(id2, tags2, 2))
	mismatches := checker.indexMismatches(300)
	expected := []ReadMismatch{
		testEntryMismatch(id, tags, 0),
		testEntryMismatch(id1, tags1, 1),
		testIdxMismatch(100),
		testIdxMismatch(200),
		testEntryMismatch(id2, tags2, 2),
		testIdxMismatch(300),
	}

	testMismatches(t, expected, mismatches)
	for _, m := range mismatches {
		m.Cleanup()
	}
}

func TestComputeMismatchForEntryStrictMode(t *testing.T) {
	buffer := buildTestBuffer()
	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t).SetStrict(true))

	mismatch, err := chk.ComputeMismatchForEntry(idxEntry("foo", "bar"))
	require.NoError(t, err)
	testMismatches(t, []ReadMismatch{
		testEntryMismatch("foo", "bar", 0)}, mismatch,
	)

	mismatch, err = chk.ComputeMismatchForEntry(idxEntry("zoom", "baz"))
	require.NoError(t, err)
	testMismatches(t, []ReadMismatch{
		testEntryMismatch("zoom", "baz", 0)}, mismatch,
	)

	_, err = chk.ComputeMismatchForEntry(idxEntry("qux", "bar"))
	require.Error(t, err)
}

func TestComputeMismatchInvariant(t *testing.T) {
	buffer := buildTestBuffer(ident.IndexChecksumBlock{
		Checksums: []int64{1},
		Marker:    []byte("foo1"),
	})

	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))
	_, err := chk.ComputeMismatchForEntry(idxEntry("bar1", "bar"))
	require.Error(t, err)
}

func TestComputeMismatchInvariantEndOfBlock(t *testing.T) {
	buffer := buildTestBuffer(ident.IndexChecksumBlock{
		Checksums: []int64{1, 2},
		Marker:    []byte("foo2"),
	})

	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))
	_, err := chk.ComputeMismatchForEntry(idxEntry("bar2", "bar"))
	require.Error(t, err)
}

func assertNoMismatch(
	t *testing.T,
	chk EntryChecksumMismatchChecker,
	entry schema.IndexEntry,
) {
	mismatch, err := chk.ComputeMismatchForEntry(entry)
	require.NoError(t, err)
	assert.Equal(t, 0, len(mismatch))
}

func TestComputeMismatchWithDelayedBuffer(t *testing.T) {
	buffer := NewIndexChecksumBlockBuffer()
	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))

	go func() {
		time.Sleep(time.Millisecond * 100)
		buffer.Push(ident.IndexChecksumBlock{
			Checksums: []int64{1},
			Marker:    []byte("foo1"),
		})
		time.Sleep(time.Millisecond * 200)
		buffer.Push(ident.IndexChecksumBlock{
			Checksums: []int64{10},
			Marker:    []byte("qux10"),
		})
		buffer.Close()
	}()

	assertNoMismatch(t, chk, idxEntry("foo1", "bar"))
	assertNoMismatch(t, chk, idxEntry("qux10", "baz"))
	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchNoMismatch(t *testing.T) {
	buffer := buildTestBuffer(ident.IndexChecksumBlock{
		Checksums: []int64{1, 2, 3},
		Marker:    []byte("foo3"),
	}, ident.IndexChecksumBlock{
		Checksums: []int64{100, 5},
		Marker:    []byte("zoo5"),
	})

	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))
	assertNoMismatch(t, chk, idxEntry("abc1", "aaa"))
	assertNoMismatch(t, chk, idxEntry("def2", "bbb"))
	assertNoMismatch(t, chk, idxEntry("foo3", "ccc"))
	assertNoMismatch(t, chk, idxEntry("qux50", "ddd50"))
	assertNoMismatch(t, chk, idxEntry("zoo5", "eee"))
	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesPrimary(t *testing.T) {
	buffer := buildTestBuffer(ident.IndexChecksumBlock{
		Checksums: []int64{1, 2, 3},
		Marker:    []byte("foo3"),
	}, ident.IndexChecksumBlock{
		Checksums: []int64{4, 5},
		Marker:    []byte("moo5"),
	}, ident.IndexChecksumBlock{
		Checksums: []int64{6, 7, 8},
		Marker:    []byte("qux8"),
	}, ident.IndexChecksumBlock{
		Checksums: []int64{9, 10},
		Marker:    []byte("zzz9"),
	})

	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))

	expected := []ReadMismatch{
		testIdxMismatch(1),
		testIdxMismatch(2),
	}

	mismatches, err := chk.ComputeMismatchForEntry(idxEntry("foo3", "ccc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(4),
		testIdxMismatch(5),
		testIdxMismatch(6),
	}

	mismatches, err = chk.ComputeMismatchForEntry(idxEntry("qux7", "ddd"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(8),
		testIdxMismatch(9),
		testIdxMismatch(10),
	}
	testMismatches(t, expected, chk.Drain())
}

func TestComputeMismatchMismatchesSecondary(t *testing.T) {
	buffer := buildTestBuffer(ident.IndexChecksumBlock{
		Checksums: []int64{3},
		Marker:    []byte("foo3"),
	}, ident.IndexChecksumBlock{
		Checksums: []int64{5},
		Marker:    []byte("goo5"),
	}, ident.IndexChecksumBlock{
		Checksums: []int64{6},
		Marker:    []byte("moo6"),
	}, ident.IndexChecksumBlock{
		Checksums: []int64{7},
		Marker:    []byte("qux7"),
	})

	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("abc1", "ccc", 1),
	}

	mismatches, err := chk.ComputeMismatchForEntry(idxEntry("abc1", "ccc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testEntryMismatch("def2", "ddd", 2),
	}

	mismatches, err = chk.ComputeMismatchForEntry(idxEntry("def2", "ddd"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testEntryMismatch("foo3", "f1", 4),
	}

	mismatches, err = chk.ComputeMismatchForEntry(idxEntry("foo3", "f1"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(5),
	}

	mismatches, err = chk.ComputeMismatchForEntry(idxEntry("moo6", "a"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(7),
		testEntryMismatch("zoo10", "z", 10),
	}

	mismatches, err = chk.ComputeMismatchForEntry(idxEntry("zoo10", "z"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesOvershoot(t *testing.T) {
	buffer := buildTestBuffer(ident.IndexChecksumBlock{
		Checksums: []int64{1, 2, 3},
		Marker:    []byte("foo3"),
	}, ident.IndexChecksumBlock{
		Checksums: []int64{4, 5, 10},
		Marker:    []byte("goo10"),
	})

	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("abc10", "ccc", 10),
	}

	mismatches, err := chk.ComputeMismatchForEntry(idxEntry("abc10", "ccc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(1),
		testIdxMismatch(2),
		testIdxMismatch(3),
		testIdxMismatch(4),
		testIdxMismatch(5),
		testIdxMismatch(10),
		testEntryMismatch("zzz20", "ccc", 20),
	}

	mismatches, err = chk.ComputeMismatchForEntry(idxEntry("zzz20", "ccc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesSecondarySkipsFirst(t *testing.T) {
	buffer := buildTestBuffer(ident.IndexChecksumBlock{
		Checksums: []int64{3},
		Marker:    []byte("foo3"),
	})

	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("foo3", "abc1", 4),
	}

	mismatches, err := chk.ComputeMismatchForEntry(idxEntry("foo3", "abc1"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesSecondaryMatchesLast(t *testing.T) {
	buffer := buildTestBuffer(ident.IndexChecksumBlock{
		Checksums: []int64{1, 2, 3},
		Marker:    []byte("foo3"),
	})

	chk := NewEntryChecksumMismatchChecker(buffer, buildOpts(t))
	expected := []ReadMismatch{
		testIdxMismatch(1),
		testIdxMismatch(2),
	}

	mismatches, err := chk.ComputeMismatchForEntry(idxEntry("foo3", "abc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}
