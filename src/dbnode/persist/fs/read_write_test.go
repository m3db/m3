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

package fs

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/bloom/v4"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testSnapshotID = uuid.Parse("bbc85a98-bd0c-47fe-8b9a-89cde1b4540f")
)

type testEntry struct {
	id   string
	tags map[string]string
	data []byte
}

func (e testEntry) ID() ident.ID {
	return ident.StringID(e.id)
}

func (e testEntry) Tags() ident.Tags {
	if e.tags == nil {
		return ident.Tags{}
	}

	// Return in sorted order for deterministic order
	var keys []string
	for key := range e.tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var tags ident.Tags
	for _, key := range keys {
		tags.Append(ident.StringTag(key, e.tags[key]))
	}

	return tags
}

type testEntries []testEntry

func (e testEntries) Less(i, j int) bool {
	return e[i].id < e[j].id
}

func (e testEntries) Len() int {
	return len(e)
}

func (e testEntries) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func newTestWriter(t *testing.T, filePathPrefix string) DataFileSetWriter {
	writer, err := NewWriter(testDefaultOpts.
		SetFilePathPrefix(filePathPrefix).
		SetWriterBufferSize(testWriterBufferSize))
	require.NoError(t, err)
	return writer
}

func writeTestData(
	t *testing.T,
	w DataFileSetWriter,
	shard uint32,
	timestamp time.Time,
	entries []testEntry,
	fileSetType persist.FileSetType,
) {
	writeTestDataWithVolume(
		t, w, shard, timestamp, 0, entries, fileSetType)
}

func writeTestDataWithVolume(
	t *testing.T,
	w DataFileSetWriter,
	shard uint32,
	timestamp time.Time,
	volume int,
	entries []testEntry,
	fileSetType persist.FileSetType,
) {
	writerOpts := DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:   testNs1ID,
			Shard:       shard,
			BlockStart:  timestamp,
			VolumeIndex: volume,
		},
		BlockSize:   testBlockSize,
		FileSetType: fileSetType,
	}

	if fileSetType == persist.FileSetSnapshotType {
		writerOpts.Snapshot.SnapshotTime = timestamp
		writerOpts.Snapshot.SnapshotID = testSnapshotID
	}

	err := w.Open(writerOpts)
	assert.NoError(t, err)

	for i := range entries {
		metadata := persist.NewMetadataFromIDAndTags(entries[i].ID(),
			entries[i].Tags(),
			persist.MetadataOptions{})
		assert.NoError(t, w.Write(metadata,
			bytesRefd(entries[i].data),
			digest.Checksum(entries[i].data)))
	}
	assert.NoError(t, w.Close())

	// Assert that any index entries released references they held
	writer, ok := w.(*writer)
	require.True(t, ok)

	// Take ref to wholly allocated index entries slice
	slice := writer.indexEntries[:cap(writer.indexEntries)]

	// Check every entry has ID and Tags nil
	for _, elem := range slice {
		assert.Equal(t, persist.Metadata{}, elem.metadata)
	}
}

type readTestType uint

const (
	readTestTypeData readTestType = iota
	readTestTypeMetadata
)

var readTestTypes = []readTestType{
	readTestTypeData,
	readTestTypeMetadata,
}

func readTestData(t *testing.T, r DataFileSetReader, shard uint32, timestamp time.Time, entries []testEntry) {
	readTestDataWithStreamingOpt(t, r, shard, timestamp, entries, false)

	sortedEntries := append(make(testEntries, 0, len(entries)), entries...)
	sort.Sort(sortedEntries)

	readTestDataWithStreamingOpt(t, r, shard, timestamp, sortedEntries, true)
}

// readTestDataWithStreamingOpt will test reading back the data matches what was written,
// note that this test also tests reuse of the reader since it first reads
// all the data then closes it, reopens and reads through again but just
// reading the metadata the second time.
// If it starts to fail during the pass that reads just the metadata it could
// be a newly introduced reader reuse bug.
func readTestDataWithStreamingOpt(
	t *testing.T,
	r DataFileSetReader,
	shard uint32,
	timestamp time.Time,
	entries []testEntry,
	streamingEnabled bool,
) {
	for _, underTest := range readTestTypes {
		if underTest == readTestTypeMetadata && streamingEnabled {
			// ATM there is no streaming support for metadata.
			continue
		}

		rOpenOpts := DataReaderOpenOptions{
			Identifier: FileSetFileIdentifier{
				Namespace:  testNs1ID,
				Shard:      shard,
				BlockStart: timestamp,
			},
			StreamingEnabled: streamingEnabled,
		}
		err := r.Open(rOpenOpts)
		require.NoError(t, err)

		require.Equal(t, len(entries), r.Entries())
		require.Equal(t, 0, r.EntriesRead())

		bloomFilter, err := r.ReadBloomFilter()
		assert.NoError(t, err)
		// Make sure the bloom filter doesn't always return true
		assert.False(t, bloomFilter.Test([]byte("some_random_data")))

		expectedEntries := uint(len(entries))
		if expectedEntries == 0 {
			expectedEntries = 1
		}
		expectedM, expectedK := bloom.EstimateFalsePositiveRate(
			expectedEntries, defaultIndexBloomFilterFalsePositivePercent)
		assert.Equal(t, expectedK, bloomFilter.K())
		// EstimateFalsePositiveRate always returns at least 1, so skip this check
		// if len entries is 0
		if len(entries) > 0 {
			assert.Equal(t, expectedM, bloomFilter.M())
		}

		for i := 0; i < r.Entries(); i++ {
			switch underTest {
			case readTestTypeData:
				id, tags, data, checksum, err := readData(t, r)
				require.NoError(t, err)

				data.IncRef()

				// Assert id
				assert.Equal(t, entries[i].id, id.String())

				// Assert tags
				tagMatcher := ident.NewTagIterMatcher(ident.NewTagsIterator(entries[i].Tags()))
				assert.True(t, tagMatcher.Matches(tags))

				assert.True(t, bytes.Equal(entries[i].data, data.Bytes()))
				assert.Equal(t, digest.Checksum(entries[i].data), checksum)

				assert.Equal(t, i+1, r.EntriesRead())

				// Verify that the bloomFilter was bootstrapped properly by making sure it
				// at least contains every ID
				assert.True(t, bloomFilter.Test(id.Bytes()))

				id.Finalize()
				tags.Close()
				data.DecRef()
				data.Finalize()

			case readTestTypeMetadata:
				id, tags, length, checksum, err := r.ReadMetadata()
				require.NoError(t, err)

				// Assert id
				assert.True(t, id.Equal(id))

				// Assert tags
				tagMatcher := ident.NewTagIterMatcher(ident.NewTagsIterator(entries[i].Tags()))
				assert.True(t, tagMatcher.Matches(tags))

				assert.Equal(t, digest.Checksum(entries[i].data), checksum)
				assert.Equal(t, len(entries[i].data), length)

				assert.Equal(t, i+1, r.MetadataRead())

				// Verify that the bloomFilter was bootstrapped properly by making sure it
				// at least contains every ID
				assert.True(t, bloomFilter.Test(id.Bytes()))

				id.Finalize()
				tags.Close()
			}
		}

		require.NoError(t, r.Close())
	}
}

func TestSimpleReadWrite(t *testing.T) {
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

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 0, testWriterStart, entries)
}

func TestCheckpointFileSizeBytesSize(t *testing.T) {
	// These values need to match so that the logic for determining whether
	// a checkpoint file is complete or not remains correct.
	require.Equal(t, digest.DigestLenBytes, CheckpointFileSizeBytes)
}

func TestDuplicateWrite(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testEntry{
		{"foo", nil, []byte{1, 2, 3}},
		{"foo", nil, []byte{4, 5, 6}},
	}

	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
		BlockSize: testBlockSize,
	}
	err := w.Open(writerOpts)
	require.NoError(t, err)

	for i := range entries {
		metadata := persist.NewMetadataFromIDAndTags(entries[i].ID(),
			entries[i].Tags(),
			persist.MetadataOptions{})
		require.NoError(t, w.Write(metadata,
			bytesRefd(entries[i].data),
			digest.Checksum(entries[i].data)))
	}
	require.Equal(t, errors.New("encountered duplicate ID: foo"), w.Close())
}

func TestReadWithReusedReader(t *testing.T) {
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

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 0, testWriterStart, entries)
	// Reuse the reader to read again
	readTestData(t, r, 0, testWriterStart, entries)
}

func TestInfoReadWrite(t *testing.T) {
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

	readInfoFileResults := ReadInfoFiles(filePathPrefix, testNs1ID, 0, 16, nil, persist.FileSetFlushType)
	require.Equal(t, 1, len(readInfoFileResults))
	for _, result := range readInfoFileResults {
		require.NoError(t, result.Err.Error())
	}

	infoFile := readInfoFileResults[0].Info
	require.True(t, testWriterStart.Equal(xtime.FromNanoseconds(infoFile.BlockStart)))
	require.Equal(t, testBlockSize, time.Duration(infoFile.BlockSize))
	require.Equal(t, int64(len(entries)), infoFile.Entries)
}

func TestInfoReadWriteVolumeIndex(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	var (
		entries = []testEntry{}
		w       = newTestWriter(t, filePathPrefix)
		volume  = 1
	)

	writeTestDataWithVolume(t, w, 0, testWriterStart, volume, entries, persist.FileSetFlushType)

	readInfoFileResults := ReadInfoFiles(filePathPrefix, testNs1ID, 0, 16, nil, persist.FileSetFlushType)
	require.Equal(t, 1, len(readInfoFileResults))
	for _, result := range readInfoFileResults {
		require.NoError(t, result.Err.Error())
	}

	infoFile := readInfoFileResults[0].Info
	require.True(t, testWriterStart.Equal(xtime.FromNanoseconds(infoFile.BlockStart)))
	require.Equal(t, volume, infoFile.VolumeIndex)
	require.Equal(t, testBlockSize, time.Duration(infoFile.BlockSize))
	require.Equal(t, int64(len(entries)), infoFile.Entries)
}

func TestInfoReadWriteSnapshot(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)
	writeTestData(t, w, 0, testWriterStart, nil, persist.FileSetSnapshotType)

	snapshotFiles, err := SnapshotFiles(filePathPrefix, testNs1ID, 0)
	require.NoError(t, err)

	require.Equal(t, 1, len(snapshotFiles))

	snapshot := snapshotFiles[0]
	snapshotTime, snapshotID, err := snapshot.SnapshotTimeAndID()
	require.NoError(t, err)
	require.True(t, testWriterStart.Equal(snapshotTime))
	require.Equal(t, testSnapshotID, snapshotID)
}

func TestReusingReaderWriter(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	allEntries := [][]testEntry{
		{
			{"foo", nil, []byte{1, 2, 3}},
			{"bar", nil, []byte{4, 5, 6}},
		},
		{
			{"baz", nil, []byte{7, 8, 9}},
		},
		{},
	}
	w := newTestWriter(t, filePathPrefix)
	for i := range allEntries {
		writeTestData(
			t, w, 0, testWriterStart.Add(time.Duration(i)*time.Hour), allEntries[i], persist.FileSetFlushType)
	}

	r := newTestReader(t, filePathPrefix)
	for i := range allEntries {
		readTestData(t, r, 0, testWriterStart.Add(time.Duration(i)*time.Hour), allEntries[i])
	}
}

func TestReusingWriterAfterWriteError(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testEntry{
		{"foo", nil, []byte{1, 2, 3}},
		{"bar", nil, []byte{4, 5, 6}},
	}
	w := newTestWriter(t, filePathPrefix)
	shard := uint32(0)
	writerOpts := DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: testWriterStart,
		},
	}
	metadata := persist.NewMetadataFromIDAndTags(entries[0].ID(),
		entries[0].Tags(),
		persist.MetadataOptions{})
	require.NoError(t, w.Open(writerOpts))

	require.NoError(t, w.Write(metadata,
		bytesRefd(entries[0].data),
		digest.Checksum(entries[0].data)))

	// Intentionally force a writer error.
	w.(*writer).err = errors.New("foo")
	metadata = persist.NewMetadataFromIDAndTags(entries[1].ID(),
		entries[1].Tags(),
		persist.MetadataOptions{})
	require.Equal(t, "foo", w.Write(metadata,
		bytesRefd(entries[1].data),
		digest.Checksum(entries[1].data)).Error())
	w.Close()

	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: testWriterStart,
		},
	}
	require.Equal(t, ErrCheckpointFileNotFound, r.Open(rOpenOpts))

	// Now reuse the writer and validate the data are written as expected.
	writeTestData(t, w, shard, testWriterStart, entries, persist.FileSetFlushType)
	readTestData(t, r, shard, testWriterStart, entries)
}

func TestWriterOnlyWritesNonNilBytes(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	metadata := persist.NewMetadataFromIDAndTags(
		ident.StringID("foo"),
		ident.Tags{},
		persist.MetadataOptions{})
	require.NoError(t, w.Open(writerOpts))

	err := w.WriteAll(metadata,
		[]checked.Bytes{
			checkedBytes([]byte{1, 2, 3}),
			nil,
			checkedBytes([]byte{4, 5, 6}),
		},
		digest.Checksum([]byte{1, 2, 3, 4, 5, 6}))
	require.NoError(t, err)

	assert.NoError(t, w.Close())

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 0, testWriterStart, []testEntry{
		{"foo", nil, []byte{1, 2, 3, 4, 5, 6}},
	})
}

func readData(t *testing.T, reader DataFileSetReader) (id ident.ID, tags ident.TagIterator, data checked.Bytes, checksum uint32, err error) {
	if reader.StreamingEnabled() {
		entry, err := reader.StreamingRead()
		if err != nil {
			return nil, nil, nil, 0, err
		}
		var tags = ident.EmptyTagIterator
		if len(entry.EncodedTags) > 0 {
			tagsDecoder := testTagDecoderPool.Get()
			tagsDecoder.Reset(checkedBytes(entry.EncodedTags))
			require.NoError(t, tagsDecoder.Err())
			tags = tagsDecoder
		}

		return entry.ID, tags, checked.NewBytes(entry.Data, nil), entry.DataChecksum, err
	}

	return reader.Read()
}

func checkedBytes(b []byte) checked.Bytes {
	r := checked.NewBytes(b, nil)
	r.IncRef()
	return r
}
