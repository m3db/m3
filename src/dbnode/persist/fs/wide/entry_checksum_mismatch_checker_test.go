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
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xhash "github.com/m3db/m3/src/x/test/hash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildTestReader(bls ...ident.IndexChecksumBlockBatch) IndexChecksumBlockBatchReader {
	ch := make(chan ident.IndexChecksumBlockBatch)
	reader := NewIndexChecksumBlockBatchReader(ch)
	go func() {
		for _, bl := range bls {
			ch <- bl
		}

		close(ch)
	}()
	return reader
}

// buildOpts builds default test options. The xhash.NewParsedIndexHasher sets
// the hash value for a schema.IndexEntry as any string represented integer
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

func idxEntry(id, tags string) schema.IndexEntry {
	return schema.IndexEntry{
		ID:          []byte(id),
		EncodedTags: []byte(tags),
	}
}

func toEntry(id, tags string, checksum int64) entryWithChecksum {
	return entryWithChecksum{
		entry:      idxEntry(id, tags),
		idChecksum: checksum,
	}
}

func testIdxMismatch(checksum int64) ReadMismatch {
	return ReadMismatch{
		Checksum: checksum,
	}
}

func testEntryMismatch(id, tags string, checksum int64) ReadMismatch {
	return ReadMismatch{
		Checksum:    checksum,
		ID:          []byte(id),
		EncodedTags: []byte(tags),
	}
}

func testMismatches(t *testing.T, expected, actual []ReadMismatch) {
	require.Equal(t, len(expected), len(actual))
	for i, ex := range expected {
		assert.Equal(t, ex, actual[i])
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
	checker.entryMismatches(toEntry(id1, tags1, 0), toEntry(id2, tags2, 1))
	checker.recordIndexMismatches(100, 200)
	checker.entryMismatches(toEntry(id3, tags3, 2))
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
	reader := buildTestReader(ident.IndexChecksumBlockBatch{
		Checksums: []int64{1},
		EndMarker: []byte("foo1"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	_, err := chk.ComputeMismatchesForEntry(idxEntry("bar1", "bar"))
	require.Error(t, err)
}

func TestComputeMismatchInvariantEndOfBlock(t *testing.T) {
	reader := buildTestReader(ident.IndexChecksumBlockBatch{
		Checksums: []int64{1, 2},
		EndMarker: []byte("foo2"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	_, err := chk.ComputeMismatchesForEntry(idxEntry("bar2", "bar"))
	require.Error(t, err)
}

func assertNoMismatch(
	t *testing.T,
	chk EntryChecksumMismatchChecker,
	entry schema.IndexEntry,
) {
	mismatch, err := chk.ComputeMismatchesForEntry(entry)
	require.NoError(t, err)
	assert.Equal(t, 0, len(mismatch))
}

func TestComputeMismatchWithDelayedReader(t *testing.T) {
	ch := make(chan ident.IndexChecksumBlockBatch)
	reader := NewIndexChecksumBlockBatchReader(ch)
	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))

	go func() {
		time.Sleep(time.Millisecond * 100)
		ch <- ident.IndexChecksumBlockBatch{
			Checksums: []int64{1},
			EndMarker: []byte("foo1"),
		}
		time.Sleep(time.Millisecond * 200)
		ch <- ident.IndexChecksumBlockBatch{
			Checksums: []int64{10},
			EndMarker: []byte("qux10"),
		}
		close(ch)
	}()

	assertNoMismatch(t, chk, idxEntry("foo1", "bar"))
	assertNoMismatch(t, chk, idxEntry("qux10", "baz"))
	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchNoMismatch(t *testing.T) {
	reader := buildTestReader(ident.IndexChecksumBlockBatch{
		Checksums: []int64{1, 2, 3},
		EndMarker: []byte("foo3"),
	}, ident.IndexChecksumBlockBatch{
		Checksums: []int64{100, 5},
		EndMarker: []byte("zoo5"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	assertNoMismatch(t, chk, idxEntry("abc1", "aaa"))
	assertNoMismatch(t, chk, idxEntry("def2", "bbb"))
	assertNoMismatch(t, chk, idxEntry("foo3", "ccc"))
	assertNoMismatch(t, chk, idxEntry("qux100", "ddd"))
	assertNoMismatch(t, chk, idxEntry("zoo5", "eee"))
	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesIndexMismatch(t *testing.T) {
	reader := buildTestReader(ident.IndexChecksumBlockBatch{
		Checksums: []int64{1, 2, 3},
		EndMarker: []byte("foo3"),
	}, ident.IndexChecksumBlockBatch{
		Checksums: []int64{4, 5},
		EndMarker: []byte("moo5"),
	}, ident.IndexChecksumBlockBatch{
		Checksums: []int64{6, 7, 8},
		EndMarker: []byte("qux8"),
	}, ident.IndexChecksumBlockBatch{
		Checksums: []int64{9, 10},
		EndMarker: []byte("zzz9"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))

	expected := []ReadMismatch{
		testIdxMismatch(1),
		testIdxMismatch(2),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(idxEntry("foo3", "ccc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(4),
		testIdxMismatch(5),
		testIdxMismatch(6),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(idxEntry("qux7", "ddd"))
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
	reader := buildTestReader(ident.IndexChecksumBlockBatch{
		Checksums: []int64{4},
		EndMarker: []byte("foo3"),
	}, ident.IndexChecksumBlockBatch{
		Checksums: []int64{5},
		EndMarker: []byte("goo5"),
	}, ident.IndexChecksumBlockBatch{
		Checksums: []int64{6},
		EndMarker: []byte("moo6"),
	}, ident.IndexChecksumBlockBatch{
		Checksums: []int64{7},
		EndMarker: []byte("qux7"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("abc1", "ccc", 1),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(idxEntry("abc1", "ccc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testEntryMismatch("def2", "ddd", 2),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(idxEntry("def2", "ddd"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testEntryMismatch("foo3", "f1", 3),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(idxEntry("foo3", "f1"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(5),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(idxEntry("moo6", "a"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	expected = []ReadMismatch{
		testIdxMismatch(7),
		testEntryMismatch("zoo10", "z", 10),
	}

	mismatches, err = chk.ComputeMismatchesForEntry(idxEntry("zoo10", "z"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesOvershoot(t *testing.T) {
	reader := buildTestReader(ident.IndexChecksumBlockBatch{
		Checksums: []int64{1, 2, 3},
		EndMarker: []byte("foo3"),
	}, ident.IndexChecksumBlockBatch{
		Checksums: []int64{4, 5, 10},
		EndMarker: []byte("goo10"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("abc10", "ccc", 10),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(idxEntry("abc10", "ccc"))
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

	mismatches, err = chk.ComputeMismatchesForEntry(idxEntry("zzz20", "ccc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesEntryMismatchSkipsFirst(t *testing.T) {
	reader := buildTestReader(ident.IndexChecksumBlockBatch{
		Checksums: []int64{4},
		EndMarker: []byte("foo3"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	expected := []ReadMismatch{
		testEntryMismatch("foo3", "abc1", 3),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(idxEntry("foo3", "abc1"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}

func TestComputeMismatchMismatchesEntryMismatchMatchesLast(t *testing.T) {
	reader := buildTestReader(ident.IndexChecksumBlockBatch{
		Checksums: []int64{1, 2, 3},
		EndMarker: []byte("foo3"),
	})

	chk := NewEntryChecksumMismatchChecker(reader, buildOpts(t))
	expected := []ReadMismatch{
		testIdxMismatch(1),
		testIdxMismatch(2),
	}

	mismatches, err := chk.ComputeMismatchesForEntry(idxEntry("foo3", "abc"))
	require.NoError(t, err)
	testMismatches(t, expected, mismatches)

	assert.Equal(t, 0, len(chk.Drain()))
}
