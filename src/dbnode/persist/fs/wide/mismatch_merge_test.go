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
	"regexp"
	"strconv"
	"sync"
	"testing"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var re = regexp.MustCompile(`[-]?\d[\d,]*[\.]?[\d{2}]*`)

type entryIter struct {
	idx     int
	entries []entry
}

func (it *entryIter) next() bool {
	it.idx = it.idx + 1
	return it.idx < len(it.entries)
}

func (it *entryIter) current() entry { return it.entries[it.idx] }

func (it *entryIter) assertExhausted(t *testing.T) {
	assert.True(t, it.idx >= len(it.entries))
}

func newEntryReaders(entries ...entry) entryReader {
	return &entryIter{idx: -1, entries: entries}
}

func buildExpectedOutputStream(
	t *testing.T, expected []ReadMismatch,
) (chan<- ReadMismatch, *sync.WaitGroup) {
	ch := make(chan ReadMismatch)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		actual := make([]ReadMismatch, 0, len(expected))
		for mismatch := range ch {
			actual = append(actual, mismatch)
			fmt.Printf("%+v\n", mismatch)
		}
		assert.Equal(t, expected, actual,
			fmt.Sprintf("mismatch lists do not match\n\nExpected: %+v\nActual:   %+v",
				expected, actual))
		wg.Done()
	}()

	return ch, &wg
}

func idxEntry(id string) entry {
	var checksum int64
	matched := re.FindAllString(id, -1)
	if len(matched) > 0 {
		i, _ := strconv.Atoi(string(matched[0]))
		checksum = int64(i)
	}

	return entry{
		entry: schema.IndexEntry{
			ID: []byte(id),
		},
		idChecksum: checksum,
	}
}

func mismatch(i int64, id []byte) ReadMismatch {
	m := ReadMismatch{Checksum: i}
	if len(id) > 0 {
		m.ID = ident.BytesID(id)
	}

	return m
}

func buildDataInputStream(
	bls []ident.IndexChecksumBlock,
) <-chan ident.IndexChecksumBlock {
	ch := make(chan ident.IndexChecksumBlock)
	go func() {
		for _, bl := range bls {
			ch <- bl
		}
		close(ch)
	}()

	return ch
}

func assertClosed(t *testing.T, in <-chan ident.IndexChecksumBlock) {
	_, isOpen := <-in
	require.False(t, isOpen)
}

func newTestStreamMismatchWriter(t *testing.T) *streamMismatchWriter {
	decodeOpts := msgpack.NewDecodingOptions().SetHash32(xtest.NewHash32(t))
	return newStreamMismatchWriter(decodeOpts, instrument.NewOptions())
}

func TestDrainRemainingBlockStreamAndClose(t *testing.T) {
	batch := []int64{1, 2}
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{
		{Checksums: []int64{3, 4}},
		{Checksums: []int64{5}},
		{Checksums: []int64{}},
		{Checksums: []int64{6}},
	})
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
		{Checksum: 3},
		{Checksum: 4},
		{Checksum: 5},
		{Checksum: 6},
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	w.drainRemainingBatchtreamAndClose(batch, inStream, outStream)
	assertClosed(t, inStream)
}

func TestReadRemainingReadersAndClose(t *testing.T) {
	entry := idxEntry("b1ar")
	reader := newEntryReaders(idxEntry("f2oo"), idxEntry("q3ux"))
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(1, []byte("bar")),
		mismatch(2, []byte("foo")),
		mismatch(3, []byte("qux")),
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	w.readRemainingReadersAndClose(entry, reader, outStream)
}

func TestEmitChecksumMismatches(t *testing.T) {
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(2, []byte("def2")),
	})

	w := newTestStreamMismatchWriter(t)
	w.emitChecksumMismatches(idxEntry("b1ar"), 1, outStream)
	w.emitChecksumMismatches(idxEntry("def2"), 1, outStream)

	// NB: outStream not closed naturally in this subfunction; close explicitly.
	close(outStream)
	wg.Wait()
}

func TestLoadNextValidIndexChecksumBatch(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{
		{Checksums: []int64{}, Marker: []byte("aaa")},
		{Checksums: []int64{3, 4}, Marker: []byte("foo")},
	})

	reader := newEntryReaders(idxEntry("abc1"), idxEntry("def2"))
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(1, []byte("abc1")),
		mismatch(2, []byte("def2")),
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	assert.True(t, reader.next())
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.Equal(t, []int64{3, 4}, bl.Checksums)
	assert.Equal(t, "foo", string(bl.Marker))
	bl, hasNext = w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.False(t, hasNext)
	assert.Equal(t, 0, len(bl.Checksums))
	assert.Equal(t, 0, len(bl.Marker))
	assertClosed(t, inStream)
}

func TestLoadNextWithExhaustedInput(t *testing.T) {
	inStream := make(chan ident.IndexChecksumBlock)
	close(inStream)

	reader := newEntryReaders(idxEntry("abc1"), idxEntry("def2"))
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(1, []byte("abc1")),
		mismatch(2, []byte("def2")),
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	assert.True(t, reader.next())
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.False(t, hasNext)
	assert.Equal(t, 0, len(bl.Checksums))
	assert.Equal(t, 0, len(bl.Marker))
}

func TestLoadNextValidIndexChecksumBlockValid(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{
		{Checksums: []int64{1, 2}, Marker: []byte("zztop")},
	})
	reader := newEntryReaders(idxEntry("abc10"))
	require.True(t, reader.next())

	// NB: outStream not used in this path; close explicitly.
	outStream, _ := buildExpectedOutputStream(t, []ReadMismatch{})
	close(outStream)

	w := newTestStreamMismatchWriter(t)

	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.True(t, hasNext)
	assert.Equal(t, []int64{1, 2}, bl.Checksums)
	assert.Equal(t, "zztop", string(bl.Marker))
}

func TestLoadNextValidIndexChecksumBlockSkipThenValid(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("aardvark"),
		Checksums: []int64{1, 2},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("zztop"),
		Checksums: []int64{3, 4},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2})
	reader := newEntryReaders(idxEntry("abc10"))
	require.True(t, reader.next())

	// NB: entire first block should be ONLY_ON_PRIMARY.
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.True(t, hasNext)
	assert.Equal(t, []int64{3, 4}, bl.Checksums)
	assert.Equal(t, "zztop", string(bl.Marker))

	// NB: outStream not closed in this path; close explicitly.
	close(outStream)
}

func TestLoadNextValidIndexChecksumBlockSkipsExhaustive(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("aardvark"),
		Checksums: []int64{1, 2},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("abc"),
		Checksums: []int64{3, 4},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2})
	reader := newEntryReaders(idxEntry("zztop10"), idxEntry("zzz0"))
	require.True(t, reader.next())

	// NB: entire first block should be ONLY_ON_PRIMARY,
	// entire secondary block should be ONLY_ON_SECONDARY.
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
		{Checksum: 3},
		{Checksum: 4},
		mismatch(10, []byte("zztop10")),
		mismatch(0, []byte("zzz0")),
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.False(t, hasNext)
	assert.Equal(t, 0, len(bl.Checksums))
	assert.Equal(t, 0, len(string(bl.Marker)))
}

func TestLoadNextValidIndexChecksumBlockLastElementMatch(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{{
		Marker:    []byte("abc"),
		Checksums: []int64{1, 2, 3},
	}})
	reader := newEntryReaders(idxEntry("abc3"))
	require.True(t, reader.next())

	// NB: values preceeding MARKER in index hash are only on primary.
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.False(t, hasNext)
	assert.Equal(t, 0, len(bl.Checksums))
	assert.Equal(t, 0, len(string(bl.Marker)))
}

func TestLoadNextValidIndexChecksumBlock(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("a"),
		Checksums: []int64{1, 2, 3},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("b"),
		Checksums: []int64{4, 5},
	}

	bl3 := ident.IndexChecksumBlock{
		Marker:    []byte("d"),
		Checksums: []int64{6, 7},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2, bl3})
	reader := newEntryReaders(idxEntry("b5"), idxEntry("c10"))
	require.True(t, reader.next())

	// Values preceeding MARKER in second index hash block are only on primary.
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 2},
		{Checksum: 3},
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	bl, hasNext := w.loadNextValidIndexChecksumBatch(inStream, reader, outStream)
	assert.True(t, hasNext)
	assert.Equal(t, bl3, bl)

	// NB: outStream not closed in this path; close explicitly.
	close(outStream)
}

func TestMergeHelper(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("a"),
		Checksums: []int64{2, 3},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("d"),
		Checksums: []int64{12, 4, 5},
	}

	bl3 := ident.IndexChecksumBlock{
		Marker:    []byte("mismatch"),
		Checksums: []int64{6},
	}

	bl4 := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []int64{1},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{
		bl1, bl2, bl3, bl4})
	mismatched := entry{
		entry: schema.IndexEntry{
			ID: []byte("mismatch"),
		},
		idChecksum: 88,
	}

	reader := newEntryReaders(
		idxEntry("b12"),
		idxEntry("d5"),
		mismatched,
		idxEntry("q7ux"))
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		// Values preceeding MARKER in second index hash block are only on primary.
		{Checksum: 2},
		{Checksum: 3},
		{Checksum: 4},
		// Value at 5 matches, not in output.
		// Value at 6 is a DATA_MISMATCH
		ReadMismatch{Checksum: 88, ID: []byte("mismatch")},
		// Value at 10 only on secondary.
		mismatch(7, []byte("qux")),
		// Value at 7 only on primary.
		{Checksum: 1},
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMergeIntermittentBlock(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexChecksumBlock{{
		Marker:    []byte("z"),
		Checksums: []int64{1, 2, 3, 4},
	}})
	reader := newEntryReaders(
		idxEntry("n2"),
		idxEntry("y4"),
	)

	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 1},
		{Checksum: 3},
		mismatch(4, []byte("y")),
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMergeIntermittentBlockMismatch(t *testing.T) {
	bl := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []int64{1, 2, 3, 4},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl})
	reader := newEntryReaders(
		idxEntry("n2"),
		idxEntry("z5"),
	)

	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(2, []byte("n")),
		mismatch(5, []byte("z")),
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMergeTrailingBlock(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("m"),
		Checksums: []int64{1, 2},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []int64{3, 5},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2})
	reader := newEntryReaders(
		idxEntry("a1"),
		idxEntry("w4"),
	)

	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 2},
		mismatch(4, []byte("w")),
		{Checksum: 3},
		{Checksum: 5},
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMerge(t *testing.T) {
	bl1 := ident.IndexChecksumBlock{
		Marker:    []byte("c"),
		Checksums: []int64{1, 2, 3},
	}

	bl2 := ident.IndexChecksumBlock{
		Marker:    []byte("f"),
		Checksums: []int64{4, 5},
	}

	bl3 := ident.IndexChecksumBlock{
		Marker:    []byte("p"),
		Checksums: []int64{6, 7, 8, 9},
	}

	bl4 := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []int64{11, 15},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl1, bl2, bl3, bl4})
	missEntry := idxEntry("c3")
	missEntry.idChecksum = 100
	reader := newEntryReaders(
		idxEntry("a1"),
		missEntry,
		idxEntry("n7"),
		idxEntry("p8"),
		idxEntry("p9"),
		idxEntry("q10"),
		idxEntry("z15"),
	)
	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		{Checksum: 2},
		ReadMismatch{Checksum: 100, ID: []byte("c")},
		{Checksum: 4},
		{Checksum: 5},
		{Checksum: 6},
		mismatch(10, []byte("q")),
		{Checksum: 11},
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}

func TestMergeIntermittentBlockJagged(t *testing.T) {
	bl := ident.IndexChecksumBlock{
		Marker:    []byte("z"),
		Checksums: []int64{1, 2, 4, 6},
	}

	inStream := buildDataInputStream([]ident.IndexChecksumBlock{bl})
	reader := newEntryReaders(
		idxEntry("b1"),
		idxEntry("f3"),
		idxEntry("w5"),
		idxEntry("z6"),
	)

	outStream, wg := buildExpectedOutputStream(t, []ReadMismatch{
		mismatch(2, nil),
		mismatch(3, nil),
		mismatch(4, nil),
		mismatch(5, nil),
	})
	defer wg.Wait()

	w := newTestStreamMismatchWriter(t)
	err := w.merge(inStream, reader, outStream)
	require.NoError(t, err)
	assertClosed(t, inStream)
}
