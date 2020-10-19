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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xhash "github.com/m3db/m3/src/x/test/hash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildTestReader(bls ...IndexChecksumBlockBatch) IndexChecksumBlockBatchReader {
	ch := make(chan IndexChecksumBlockBatch)
	reader := NewIndexChecksumBlockBatchReader(ch)
	go func() {
		for _, bl := range bls {
			ch <- bl
		}

		close(ch)
	}()
	return reader
}

// buildOpts builds default test options. The NewParsedIndexHasher sets
// the hash value for a xio.IndexEntry as any string represented integer
// values in the entry ID + entry tags.
func buildOpts(t *testing.T) Options {
	decodingOpts := msgpack.NewDecodingOptions().
		SetIndexEntryHasher(xhash.NewParsedIndexHasher(t))
	opts := NewOptions().
		SetBatchSize(2).
		SetDecodingOptions(decodingOpts).
		SetInstrumentOptions(instrument.NewOptions())
	require.NoError(t, opts.Validate())
	return opts
}

func toChecksum(id, tags string, checksum int64) xio.IndexChecksum {
	return xio.IndexChecksum{
		ID:               ident.StringID(id),
		EncodedTags:      checked.NewBytes([]byte(tags), checked.NewBytesOptions()),
		MetadataChecksum: checksum,
	}
}

func testIdxMismatch(checksum int64) ReadMismatch {
	return ReadMismatch{
		IndexChecksum: xio.IndexChecksum{
			MetadataChecksum: checksum,
		},
	}
}

func testEntryMismatch(id, tags string, checksum int64) ReadMismatch {
	return ReadMismatch{
		IndexChecksum: toChecksum(id, tags, checksum),
	}
}

func testMismatches(t *testing.T, expected, actual []ReadMismatch) {
	require.Equal(t, len(expected), len(actual))
	for i, ex := range expected {
		assert.Equal(t, ex.ID, actual[i].ID)
		assert.Equal(t, ex.Size, actual[i].Size)
		assert.Equal(t, ex.Offset, actual[i].Offset)
		assert.Equal(t, ex.DataChecksum, actual[i].DataChecksum)
		assert.Equal(t, ex.MetadataChecksum, actual[i].MetadataChecksum)
	}
}

func TestEmitMismatches(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	reader := buildTestReader()
	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	checker, ok := chk.(*entryChecksumMismatchChecker)
	require.True(t, ok)

	id1, tags1 := "foo1", "encoded-tags1"
	id2, tags2 := "foo2", "encoded-tags2"
	id3, tags3 := "foo3", "encoded-tags3"
	checker.checksumMismatches(toChecksum(id1, tags1, 0), toChecksum(id2, tags2, 1))
	checker.recordIndexMismatches(100, 200)
	checker.checksumMismatches(toChecksum(id3, tags3, 2))
	checker.recordIndexMismatches(300)

	expected := []ReadMismatch{
		testEntryMismatch(id1, tags1, 0),
		testEntryMismatch(id2, tags2, 1),
		testIdxMismatch(100),
		testIdxMismatch(200),
		testEntryMismatch(id3, tags3, 2),
		testIdxMismatch(300),
	}

	testMismatches(t, expected, checker.mismatches)
}

func TestComputeMismatchInvariant(t *testing.T) {
	reader := buildTestReader(IndexChecksumBlockBatch{
		Checksums: []int64{1},
		EndMarker: []byte("foo1"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	_, err := chk.ComputeMismatchesForEntry(toChecksum("bar1", "bar", 1))
	require.Error(t, err)
}

func TestComputeMismatchInvariantEndOfBlock(t *testing.T) {
	reader := buildTestReader(IndexChecksumBlockBatch{
		Checksums: []int64{1, 2},
		EndMarker: []byte("foo2"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	_, err := chk.ComputeMismatchesForEntry(toChecksum("bar2", "bar", 2))
	require.Error(t, err)
}

func assertNoMismatch(
	t *testing.T,
	chk EntryChecksumMismatchChecker,
	checksum xio.IndexChecksum,
) {
	mismatch, err := chk.ComputeMismatchesForEntry(checksum)
	require.NoError(t, err)
	assert.Equal(t, 0, len(mismatch))
}

func TestComputeMismatchWithDelayedReader(t *testing.T) {
	ch := make(chan IndexChecksumBlockBatch)
	reader := NewIndexChecksumBlockBatchReader(ch)
	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))

	go func() {
		time.Sleep(time.Millisecond * 100)
		ch <- IndexChecksumBlockBatch{
			Checksums: []int64{1},
			EndMarker: []byte("foo1"),
		}
		time.Sleep(time.Millisecond * 200)
		ch <- IndexChecksumBlockBatch{
			Checksums: []int64{10},
			EndMarker: []byte("qux10"),
		}
		close(ch)
	}()

	assertNoMismatch(t, chk, toChecksum("foo1", "bar", 1))
	assertNoMismatch(t, chk, toChecksum("qux10", "baz", 10))
	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchNoMismatch(t *testing.T) {
	reader := buildTestReader(IndexChecksumBlockBatch{
		Checksums: []int64{1, 2, 3},
		EndMarker: []byte("foo3"),
	}, IndexChecksumBlockBatch{
		Checksums: []int64{100, 5},
		EndMarker: []byte("zoo5"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	assertNoMismatch(t, chk, toChecksum("abc1", "aaa", 1))
	assertNoMismatch(t, chk, toChecksum("def2", "bbb", 2))
	assertNoMismatch(t, chk, toChecksum("foo3", "ccc", 3))
	assertNoMismatch(t, chk, toChecksum("qux100", "ddd", 100))
	assertNoMismatch(t, chk, toChecksum("zoo5", "eee", 5))
	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesIndexMismatch(t *testing.T) {
	reader := buildTestReader(IndexChecksumBlockBatch{
		Checksums: []int64{1, 2, 3},
		EndMarker: []byte("foo3"),
	}, IndexChecksumBlockBatch{
		Checksums: []int64{4, 5},
		EndMarker: []byte("moo5"),
	}, IndexChecksumBlockBatch{
		Checksums: []int64{6, 7, 8},
		EndMarker: []byte("qux8"),
	}, IndexChecksumBlockBatch{
		Checksums: []int64{9, 10},
		EndMarker: []byte("zzz9"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))

	expected := []ReadMismatch{
		testIdxMismatch(1),
		testIdxMismatch(2),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(toChecksum("foo3", "ccc", 3))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(4),
		testIdxMismatch(5),
		testIdxMismatch(6),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(toChecksum("qux7", "ddd", 7))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(8),
		testIdxMismatch(9),
		testIdxMismatch(10),
	}
	testMismatches(t, expected, chk.Drain())
}

func TestComputeMismatchMismatchesEntryMismatches(t *testing.T) {
	reader := buildTestReader(IndexChecksumBlockBatch{
		Checksums: []int64{4},
		EndMarker: []byte("foo3"),
	}, IndexChecksumBlockBatch{
		Checksums: []int64{5},
		EndMarker: []byte("goo5"),
	}, IndexChecksumBlockBatch{
		Checksums: []int64{6},
		EndMarker: []byte("moo6"),
	}, IndexChecksumBlockBatch{
		Checksums: []int64{7},
		EndMarker: []byte("qux7"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("abc1", "ccc", 1),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(toChecksum("abc1", "ccc", 1))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testEntryMismatch("def2", "ddd", 2),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(toChecksum("def2", "ddd", 2))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testEntryMismatch("foo3", "f1", 3),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(toChecksum("foo3", "f1", 3))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(5),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(toChecksum("moo6", "a", 6))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(7),
		testEntryMismatch("zoo10", "z", 10),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(toChecksum("zoo10", "z", 10))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesOvershoot(t *testing.T) {
	reader := buildTestReader(IndexChecksumBlockBatch{
		Checksums: []int64{1, 2, 3},
		EndMarker: []byte("foo3"),
	}, IndexChecksumBlockBatch{
		Checksums: []int64{4, 5, 10},
		EndMarker: []byte("goo10"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("abc10", "ccc", 10),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(toChecksum("abc10", "ccc", 10))
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

	mismatches, err = chk.ComputeMismatchesForEntry(toChecksum("zzz20", "ccc", 20))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesEntryMismatchSkipsFirst(t *testing.T) {
	reader := buildTestReader(IndexChecksumBlockBatch{
		Checksums: []int64{4},
		EndMarker: []byte("foo3"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("foo3", "abc", 3),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(toChecksum("foo3", "abc", 3))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesEntryMismatchMatchesLast(t *testing.T) {
	reader := buildTestReader(IndexChecksumBlockBatch{
		Checksums: []int64{1, 2, 3},
		EndMarker: []byte("foo3"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	expected := []ReadMismatch{
		testIdxMismatch(1),
		testIdxMismatch(2),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(toChecksum("foo3", "abc", 3))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}
