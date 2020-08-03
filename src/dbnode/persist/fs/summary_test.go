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

package fs

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildSummaryMap(
	t *testing.T,
	filePathPrefix string,
	exSize int,
) map[uint64]int64 {
	s, err := NewSummarizer(testDefaultOpts.
		SetFilePathPrefix(filePathPrefix).
		SetInfoReaderBufferSize(testReaderBufferSize).
		SetDataReaderBufferSize(testReaderBufferSize))
	require.NoError(t, err)

	opts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
		OrderedByIndex: true,
	}

	err = s.Open(opts)
	require.NoError(t, err)

	require.Equal(t, exSize, s.Entries())
	require.Equal(t, 0, s.MetadataRead())

	summaryMap := make(map[uint64]int64, exSize)
	for i := 0; i < exSize; i++ {
		summary, err := s.ReadMetadata()
		require.NoError(t, err)
		summaryMap[summary.IDHash] = summary.DataChecksum
	}

	require.Equal(t, exSize, s.Entries())
	require.Equal(t, exSize, s.MetadataRead())
	require.NoError(t, s.Close())
	return summaryMap
}

func openSummaryReader(
	t *testing.T,
	filePathPrefix string,
	summaryMap map[uint64]int64,
) DataFileSetReader {
	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
		OrderedByIndex:  true,
		IndexSummaryMap: summaryMap,
	}

	err := r.Open(rOpenOpts)
	require.NoError(t, err)

	return r
}

func TestSimpleReadWriteSummary(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testEntry{
		{"foo", nil, []byte{1, 2, 3}},
		{"bar", nil, []byte{4, 5, 6}},
		{"baz", nil, make([]byte, 65536)},
		{"cat", nil, make([]byte, 100000)},
		{"foo+bar=baz,qux=qaz", map[string]string{
			"bar": "baz",
			"qux": "qaz",
		}, []byte{7, 8, 9}},
	}

	w := newTestWriter(t, filePathPrefix)
	writeTestData(t, w, 0, testWriterStart, entries, persist.FileSetFlushType)

	summaryMap := buildSummaryMap(t, filePathPrefix, len(entries))
	r := openSummaryReader(t, filePathPrefix, summaryMap)

	readMismatch, err := r.ReadMismatch()
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, MismatchNone, readMismatch.Type)

	assert.NoError(t, r.Close())
}

func TestSimpleMismatchedReadWriteSummary(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testEntry{
		{"foo", nil, []byte{1, 2, 3}},
		{"bar", nil, []byte{4, 5, 6}},
		{"baz", nil, make([]byte, 65536)},
		{"cat", nil, make([]byte, 100000)},
		{"foo+bar=baz,qux=qaz", map[string]string{
			"bar": "baz",
			"qux": "qaz",
		}, []byte{7, 8, 9}},
	}

	w := newTestWriter(t, filePathPrefix)
	writeTestData(t, w, 0, testWriterStart, entries, persist.FileSetFlushType)

	dirCompare := createTempDir(t)
	filePathPrefixCompare := filepath.Join(dirCompare, "")
	defer os.RemoveAll(dirCompare)

	updatedLength := 1337
	entriesCompare := []testEntry{
		{"foo", nil, []byte{1, 2, 3}},
		// missing: {"bar", nil, []byte{4, 5, 6}},
		{"baz", nil, make([]byte, 65536)},
		{"cat", nil, make([]byte, updatedLength)}, // different payload
		{"foo+bar=baz,qux=qaz", map[string]string{
			"bar": "baz",
			"qux": "qaz",
		}, []byte{7, 8, 9}},
		{"qux", nil, []byte{40, 50, 60}}, // only in this set
	}

	wCompare := newTestWriter(t, filePathPrefixCompare)
	writeTestData(t, wCompare, 0, testWriterStart, entriesCompare, persist.FileSetFlushType)

	summaryMap := buildSummaryMap(t, filePathPrefix, len(entries))
	r := openSummaryReader(t, filePathPrefixCompare, summaryMap)

	assert.Equal(t, len(entries), len(r.RemainingMismatches()))

	// NB: `bar` will not be the first mismatch, as it is not found in the set
	// being compared. Instead it should show up as unfulfilled at the end.

	// Mismatch 1: `cat` has a different payload.
	readMismatch, err := r.ReadMismatch()
	require.NoError(t, err)
	assert.Equal(t, MismatchChecksumMismatch, readMismatch.Type)
	assert.Equal(t, "cat", readMismatch.ID.String())
	readMismatch.Data.IncRef()
	assert.Equal(t, updatedLength, readMismatch.Data.Len())
	readMismatch.Data.DecRef()

	// Mismatch 2: `qux` exists only in comparison set.
	readMismatch, err = r.ReadMismatch()
	require.NoError(t, err)
	assert.Equal(t, MismatchMissingInSummary, readMismatch.Type)
	assert.Equal(t, "qux", readMismatch.ID.String())
	readMismatch.Data.IncRef()
	assert.Equal(t, []byte{40, 50, 60}, readMismatch.Data.Bytes())
	readMismatch.Data.DecRef()

	readMismatch, err = r.ReadMismatch()
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, MismatchNone, readMismatch.Type)

	// Mismatch 3: `bar` existing in original set but not this set.
	mismatches := r.RemainingMismatches()
	assert.Equal(t, 1, len(mismatches))
	readMismatch = mismatches[0]

	assert.Equal(t, MismatchOnlyInSummary, readMismatch.Type)
	assert.Equal(t, xxhash.Sum64([]byte("bar")), readMismatch.IDHash)
	assert.NoError(t, r.Close())
}
