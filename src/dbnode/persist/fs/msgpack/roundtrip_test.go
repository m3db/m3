// Copyright (c) 2016 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package msgpack

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/pool"
	xtest "github.com/m3db/m3/src/x/test"
	xhash "github.com/m3db/m3/src/x/test/hash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testIndexInfo = schema.IndexInfo{
		BlockStart:   time.Now().UnixNano(),
		BlockSize:    int64(2 * time.Hour),
		Entries:      2000000,
		MajorVersion: schema.MajorVersion,
		Summaries: schema.IndexSummariesInfo{
			Summaries: 123,
		},
		BloomFilter: schema.IndexBloomFilterInfo{
			NumElementsM: 2075674,
			NumHashesK:   7,
		},
		SnapshotTime: time.Now().UnixNano(),
		FileType:     persist.FileSetSnapshotType,
		SnapshotID:   []byte("some_bytes"),
		VolumeIndex:  1,
		MinorVersion: schema.MinorVersion,
	}

	testIndexEntryChecksum = int64(2611877657)
	testIndexEntry         = schema.IndexEntry{
		Index:         234,
		ID:            []byte("testIndexEntry"),
		Size:          5456,
		Offset:        2390423,
		DataChecksum:  134245634534,
		IndexChecksum: testIndexEntryChecksum,
		EncodedTags:   []byte("testEncodedTags"),
	}

	testIndexSummary = schema.IndexSummary{
		Index:            234,
		ID:               []byte("testIndexSummary"),
		IndexEntryOffset: 2390423,
	}

	testLogInfo = schema.LogInfo{
		Index: 234,
	}

	testLogEntry = schema.LogEntry{
		Create:     time.Now().UnixNano(),
		Index:      9345,
		Metadata:   []byte("testMetadata"),
		Timestamp:  time.Now().Add(time.Minute).UnixNano(),
		Value:      903.234,
		Unit:       9,
		Annotation: []byte("testAnnotation"),
	}

	testLogMetadata = schema.LogMetadata{
		ID:          []byte("testLogMetadata"),
		Namespace:   []byte("testNamespace"),
		Shard:       123,
		EncodedTags: []byte("testLogMetadataTags"),
	}

	// NB: 100 is the expected hash for checksums of `testWideEntry`
	testMetadataChecksum = int64(100)

	testWideEntry = schema.WideEntry{
		IndexEntry: schema.IndexEntry{
			Index:         234,
			ID:            []byte("test100"),
			Size:          5456,
			Offset:        2390423,
			DataChecksum:  134245634534,
			IndexChecksum: 631574095,
			EncodedTags:   []byte("2000"),
		},
		MetadataChecksum: testMetadataChecksum,
	}
)

func TestIndexInfoRoundtrip(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V5 decoding code can handle the V1 file format.
func TestIndexInfoRoundTripBackwardsCompatibilityV1(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{EncodeLegacyIndexInfoVersion: LegacyEncodingIndexVersionV1}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V1,
	// as well as the fields that were added in versions after V1,
	// and then restore them at the end of the test - This is required
	// because the new decoder won't try and read the new fields from
	// the old file format
	var (
		currSnapshotTime = testIndexInfo.SnapshotTime
		currFileType     = testIndexInfo.FileType
		currSnapshotID   = testIndexInfo.SnapshotID
		currVolumeIndex  = testIndexInfo.VolumeIndex
		currMinorVersion = testIndexInfo.MinorVersion
	)
	testIndexInfo.SnapshotTime = 0
	testIndexInfo.FileType = 0
	testIndexInfo.SnapshotID = nil
	testIndexInfo.VolumeIndex = 0
	testIndexInfo.MinorVersion = 0
	defer func() {
		testIndexInfo.SnapshotTime = currSnapshotTime
		testIndexInfo.FileType = currFileType
		testIndexInfo.SnapshotID = currSnapshotID
		testIndexInfo.VolumeIndex = currVolumeIndex
		testIndexInfo.MinorVersion = currMinorVersion
	}()

	enc.EncodeIndexInfo(testIndexInfo)
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V1 decoder code can handle the V5 file format.
func TestIndexInfoRoundTripForwardsCompatibilityV1(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{DecodeLegacyIndexInfoVersion: LegacyEncodingIndexVersionV1}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V1
	// and then restore them at the end of the test - This is required
	// because the old decoder won't read the new fields
	var (
		currSnapshotTime = testIndexInfo.SnapshotTime
		currFileType     = testIndexInfo.FileType
		currSnapshotID   = testIndexInfo.SnapshotID
		currVolumeIndex  = testIndexInfo.VolumeIndex
		currMinorVersion = testIndexInfo.MinorVersion
	)

	enc.EncodeIndexInfo(testIndexInfo)

	// Make sure to zero them before we compare, but after we have
	// encoded the data
	testIndexInfo.SnapshotTime = 0
	testIndexInfo.FileType = 0
	testIndexInfo.SnapshotID = nil
	testIndexInfo.VolumeIndex = 0
	testIndexInfo.MinorVersion = 0
	defer func() {
		testIndexInfo.SnapshotTime = currSnapshotTime
		testIndexInfo.FileType = currFileType
		testIndexInfo.SnapshotID = currSnapshotID
		testIndexInfo.VolumeIndex = currVolumeIndex
		testIndexInfo.MinorVersion = currMinorVersion
	}()

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V5 decoding code can handle the V2 file format.
func TestIndexInfoRoundTripBackwardsCompatibilityV2(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{EncodeLegacyIndexInfoVersion: LegacyEncodingIndexVersionV2}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V2,
	// and then restore them at the end of the test - This is required
	// because the new decoder won't try and read the new fields from
	// the old file format.
	var (
		currSnapshotTime = testIndexInfo.SnapshotTime
		currFileType     = testIndexInfo.FileType
		currSnapshotID   = testIndexInfo.SnapshotID
		currVolumeIndex  = testIndexInfo.VolumeIndex
		currMinorVersion = testIndexInfo.MinorVersion
	)
	testIndexInfo.SnapshotTime = 0
	testIndexInfo.FileType = 0
	testIndexInfo.SnapshotID = nil
	testIndexInfo.VolumeIndex = 0
	testIndexInfo.MinorVersion = 0
	defer func() {
		testIndexInfo.SnapshotTime = currSnapshotTime
		testIndexInfo.FileType = currFileType
		testIndexInfo.SnapshotID = currSnapshotID
		testIndexInfo.VolumeIndex = currVolumeIndex
		testIndexInfo.MinorVersion = currMinorVersion
	}()

	enc.EncodeIndexInfo(testIndexInfo)
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V2 decoder code can handle the V5 file format.
func TestIndexInfoRoundTripForwardsCompatibilityV2(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{DecodeLegacyIndexInfoVersion: LegacyEncodingIndexVersionV2}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V2
	// and then restore them at the end of the test - This is required
	// because the old decoder won't read the new fields.
	currSnapshotID := testIndexInfo.SnapshotID
	currVolumeIndex := testIndexInfo.VolumeIndex
	currMinorVersion := testIndexInfo.MinorVersion

	enc.EncodeIndexInfo(testIndexInfo)

	// Make sure to zero them before we compare, but after we have
	// encoded the data.
	testIndexInfo.SnapshotID = nil
	testIndexInfo.VolumeIndex = 0
	testIndexInfo.MinorVersion = 0
	defer func() {
		testIndexInfo.SnapshotID = currSnapshotID
		testIndexInfo.VolumeIndex = currVolumeIndex
		testIndexInfo.MinorVersion = currMinorVersion
	}()

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V5 decoding code can handle the V3 file format.
func TestIndexInfoRoundTripBackwardsCompatibilityV3(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{EncodeLegacyIndexInfoVersion: LegacyEncodingIndexVersionV3}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V3,
	// and then restore them at the end of the test - This is required
	// because the new decoder won't try and read the new fields from
	// the old file format.
	var (
		currVolumeIndex  = testIndexInfo.VolumeIndex
		currMinorVersion = testIndexInfo.MinorVersion
	)
	testIndexInfo.VolumeIndex = 0
	testIndexInfo.MinorVersion = 0
	defer func() {
		testIndexInfo.VolumeIndex = currVolumeIndex
		testIndexInfo.MinorVersion = currMinorVersion
	}()

	enc.EncodeIndexInfo(testIndexInfo)
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V3 decoder code can handle the V5 file format.
func TestIndexInfoRoundTripForwardsCompatibilityV3(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{DecodeLegacyIndexInfoVersion: LegacyEncodingIndexVersionV3}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V3
	// and then restore them at the end of the test - This is required
	// because the old decoder won't read the new fields.
	currVolumeIndex := testIndexInfo.VolumeIndex
	currMinorVersion := testIndexInfo.MinorVersion

	enc.EncodeIndexInfo(testIndexInfo)

	// Make sure to zero them before we compare, but after we have
	// encoded the data.
	testIndexInfo.VolumeIndex = 0
	testIndexInfo.MinorVersion = 0
	defer func() {
		testIndexInfo.VolumeIndex = currVolumeIndex
		testIndexInfo.MinorVersion = currMinorVersion
	}()

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V5 decoding code can handle the V4 file format.
func TestIndexInfoRoundTripBackwardsCompatibilityV4(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{EncodeLegacyIndexInfoVersion: LegacyEncodingIndexVersionV4}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V4,
	// and then restore them at the end of the test - This is required
	// because the new decoder won't try and read the new fields from
	// the old file format.
	currMinorVersion := testIndexInfo.MinorVersion

	testIndexInfo.MinorVersion = 0
	defer func() {
		testIndexInfo.MinorVersion = currMinorVersion
	}()

	enc.EncodeIndexInfo(testIndexInfo)
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V4 decoder code can handle the V5 file format.
func TestIndexInfoRoundTripForwardsCompatibilityV4(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{DecodeLegacyIndexInfoVersion: LegacyEncodingIndexVersionV4}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V4
	// and then restore them at the end of the test - This is required
	// because the old decoder won't read the new fields.
	currMinorVersion := testIndexInfo.MinorVersion

	enc.EncodeIndexInfo(testIndexInfo)

	// Make sure to zero them before we compare, but after we have
	// encoded the data.
	testIndexInfo.MinorVersion = 0
	defer func() {
		testIndexInfo.MinorVersion = currMinorVersion
	}()

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

func TestIndexEntryRoundtrip(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry(nil)
	require.NoError(t, err)
	require.Equal(t, testIndexEntry, res)
}

func TestIndexEntryIntoWideEntryRoundtripWithBytesPool(t *testing.T) {
	var (
		pool = pool.NewBytesPool(nil, nil)
		enc  = NewEncoder()
		dec  = NewDecoder(NewDecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t)))
	)
	pool.Init()

	require.NoError(t, enc.EncodeIndexEntry(testWideEntry.IndexEntry))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	checksum, _, err := dec.DecodeToWideEntry(testWideEntry.ID, pool)
	require.NoError(t, err)
	require.Equal(t, testWideEntry.IndexEntry, checksum.IndexEntry)
	require.Equal(t, testWideEntry.MetadataChecksum, checksum.MetadataChecksum)
}

func TestIndexEntryIntoWideEntryRoundtripWithoutBytesPool(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(NewDecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t)))
	)

	require.NoError(t, enc.EncodeIndexEntry(testWideEntry.IndexEntry))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	checksum, _, err := dec.DecodeToWideEntry(testWideEntry.ID, nil)
	require.NoError(t, err)
	require.Equal(t, testWideEntry.IndexEntry, checksum.IndexEntry)
	require.Equal(t, testWideEntry.MetadataChecksum, checksum.MetadataChecksum)
}

// Make sure the V3 decoding code can handle the V1 file format.
func TestIndexEntryRoundTripBackwardsCompatibilityV1(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		opts = LegacyEncodingOptions{
			EncodeLegacyIndexEntryVersion: LegacyEncodingIndexEntryVersionV1,
			DecodeLegacyIndexEntryVersion: LegacyEncodingIndexEntryVersionCurrent}
		enc = newEncoder(opts)
		dec = newDecoder(opts, NewDecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t)))
	)

	// Set the default values on the fields that did not exist in V1
	// and then restore them at the end of the test - This is required
	// because the new decoder won't try and read the new fields from
	// the old file format.
	currEncodedTags := testWideEntry.EncodedTags

	testWideEntry.EncodedTags = nil

	defer func() {
		testWideEntry.EncodedTags = currEncodedTags
	}()

	err := enc.EncodeIndexEntry(testWideEntry.IndexEntry)
	require.NoError(t, err)

	bytes := enc.Bytes()
	cloned := append(make([]byte, 0, len(bytes)), bytes...)
	dec.Reset(NewByteDecoderStream(bytes))
	res, err := dec.DecodeIndexEntry(nil)
	require.NoError(t, err)
	expected := testWideEntry.IndexEntry
	expected.IndexChecksum = 0
	require.Equal(t, expected, res)

	// Wide Entry decoding should fail since it requires tags for now.
	dec.Reset(NewByteDecoderStream(cloned))
	pool := pool.NewMockBytesPool(ctrl)
	idLength := len(testWideEntry.ID)
	idBytes := make([]byte, idLength)
	pool.EXPECT().Get(idLength).Return(idBytes)
	pool.EXPECT().Put(idBytes)
	_, status, err := dec.DecodeToWideEntry(testWideEntry.ID, pool)
	require.Error(t, err)
	assert.Equal(t, ErrorLookupStatus, status)
}

// Make sure the V1 decoder code can handle the V3 file format.
func TestIndexEntryRoundTripForwardsCompatibilityV1(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		opts = LegacyEncodingOptions{
			DecodeLegacyIndexEntryVersion: LegacyEncodingIndexEntryVersionV1}
		enc = newEncoder(opts)
		dec = newDecoder(opts, NewDecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t)))
	)

	// Set the default values on the fields that did not exist in V1
	// and then restore them at the end of the test - This is required
	// because the old decoder won't read the new fields.
	currEncodedTags := testWideEntry.EncodedTags

	err := enc.EncodeIndexEntry(testWideEntry.IndexEntry)
	require.NoError(t, err)

	// Make sure to zero them before we compare, but after we have
	// encoded the data.
	expected := testWideEntry.IndexEntry
	expected.EncodedTags = nil
	defer func() {
		expected.EncodedTags = currEncodedTags
	}()

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry(nil)
	require.NoError(t, err)

	expected.IndexChecksum = 0
	require.Equal(t, expected, res)

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	pool := pool.NewMockBytesPool(ctrl)
	idLength := len(testWideEntry.ID)
	idBytes := make([]byte, idLength)
	pool.EXPECT().Get(idLength).Return(idBytes)
	pool.EXPECT().Put(idBytes)
	_, status, err := dec.DecodeToWideEntry(testWideEntry.ID, pool)
	require.Error(t, err)
	assert.Equal(t, ErrorLookupStatus, status)
}

// Make sure the V3 decoding code can handle the V2 file format.
func TestIndexEntryRoundTripBackwardsCompatibilityV2(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{EncodeLegacyIndexEntryVersion: LegacyEncodingIndexEntryVersionV2,
			DecodeLegacyIndexEntryVersion: LegacyEncodingIndexEntryVersionCurrent}
		enc = newEncoder(opts)
		dec = newDecoder(opts, NewDecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t)))
	)

	// The additional field added to V3 is the index entry checksum that's transparently used by the encoder
	// and decoder and is never set on the IndexEntry struct. Therefore, no need to zero out any field in the struct
	// to make a comparison.

	err := enc.EncodeIndexEntry(testWideEntry.IndexEntry)
	require.NoError(t, err)
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry(nil)
	require.NoError(t, err)
	expected := testWideEntry.IndexEntry
	expected.IndexChecksum = 0
	require.Equal(t, expected, res)

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	chk, status, err := dec.DecodeToWideEntry(testWideEntry.ID, nil)
	require.NoError(t, err)
	assert.Equal(t, MatchedLookupStatus, status)
	ex := testWideEntry.IndexEntry
	// This file version does not have an IndexChecksum field.
	ex.IndexChecksum = 0
	require.Equal(t, ex, chk.IndexEntry)
	require.Equal(t, testWideEntry.MetadataChecksum, chk.MetadataChecksum)
}

// Make sure the V2 decoder code can handle the V3 file format.
func TestIndexEntryRoundTripForwardsCompatibilityV2(t *testing.T) {
	var (
		opts = LegacyEncodingOptions{DecodeLegacyIndexEntryVersion: LegacyEncodingIndexEntryVersionV2}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, NewDecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t)))
	)

	// The additional field added to V3 is the index entry checksum that's transparently used by the encoder
	// and decoder and is never set on the IndexEntry struct. Therefore, no need to zero out any field in the struct
	// to make a comparison.

	err := enc.EncodeIndexEntry(testWideEntry.IndexEntry)
	require.NoError(t, err)
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry(nil)
	require.NoError(t, err)
	expected := testWideEntry.IndexEntry
	expected.IndexChecksum = 0
	require.Equal(t, expected, res)

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	chk, status, err := dec.DecodeToWideEntry(testWideEntry.ID, nil)
	require.NoError(t, err)
	assert.Equal(t, MatchedLookupStatus, status)
	ex := testWideEntry.IndexEntry
	// This file version does not have an IndexChecksum field.
	ex.IndexChecksum = 0
	require.Equal(t, ex, chk.IndexEntry)
	require.Equal(t, testWideEntry.MetadataChecksum, chk.MetadataChecksum)
}

func TestIndexSummaryRoundtrip(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeIndexSummary(testIndexSummary))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, _, err := dec.DecodeIndexSummary()
	require.NoError(t, err)
	require.Equal(t, testIndexSummary, res)
}

func TestLogInfoRoundtrip(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeLogInfo(testLogInfo))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeLogInfo()
	require.NoError(t, err)
	require.Equal(t, testLogInfo, res)
}

func TestLogEntryRoundtrip(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeLogEntry(testLogEntry))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeLogEntry()
	require.NoError(t, err)
	require.Equal(t, testLogEntry, res)
}

func TestLogEntryRoundtripUniqueIndexAndRemaining(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeLogEntry(testLogEntry))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	create, idx, err := dec.DecodeLogEntryUniqueIndex()
	require.NoError(t, err)

	res, err := dec.DecodeLogEntryRemaining(create, idx)
	require.NoError(t, err)

	res.Index = idx
	require.Equal(t, testLogEntry, res)
}

func TestLogMetadataRoundtrip(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeLogMetadata()
	require.NoError(t, err)
	require.Equal(t, testLogMetadata, res)
}

func TestMultiTypeRoundtripStress(t *testing.T) {
	var (
		enc      = NewEncoder()
		hasher   = xhash.NewParsedIndexHasher(t)
		dec      = NewDecoder(NewDecodingOptions().SetIndexEntryHasher(hasher))
		iter     = 10000
		res      interface{}
		err      error
		expected []interface{}
		output   []interface{}
	)
	for i := 0; i < iter; i++ {
		switch i % 6 {
		case 0:
			require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
			expected = append(expected, testIndexInfo)
		case 1:
			require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
			expected = append(expected, testIndexEntry)
		case 2:
			require.NoError(t, enc.EncodeLogInfo(testLogInfo))
			expected = append(expected, testLogInfo)
		case 3:
			require.NoError(t, enc.EncodeLogEntry(testLogEntry))
			expected = append(expected, testLogEntry)
		case 4:
			require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
			expected = append(expected, testLogMetadata)
		case 5:
			require.NoError(t, enc.EncodeIndexEntry(testWideEntry.IndexEntry))
			expected = append(expected, testMetadataChecksum)
		}
	}

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	for i := 0; i < iter; i++ {
		switch i % 6 {
		case 0:
			res, err = dec.DecodeIndexInfo()
		case 1:
			res, err = dec.DecodeIndexEntry(nil)
		case 2:
			res, err = dec.DecodeLogInfo()
		case 3:
			res, err = dec.DecodeLogEntry()
		case 4:
			res, err = dec.DecodeLogMetadata()
		case 5:
			var (
				e schema.WideEntry
				s WideEntryLookupStatus
			)
			e, s, err = dec.DecodeToWideEntry(testWideEntry.ID, nil)
			assert.Equal(t, s, MatchedLookupStatus)
			res = e.MetadataChecksum
		}
		require.NoError(t, err)
		output = append(output, res)
	}

	require.Equal(t, expected, output)
}
