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
	}

	testIndexEntry = schema.IndexEntry{
		Index:       234,
		ID:          []byte("testIndexEntry"),
		Size:        5456,
		Offset:      2390423,
		Checksum:    134245634534,
		EncodedTags: []byte("testEncodedTags"),
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
)

func TestIndexInfoRoundtrip(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V3 decoding code can handle the V1 file format.
func TestIndexInfoRoundTripBackwardsCompatibilityV1(t *testing.T) {
	var (
		opts = legacyEncodingOptions{encodeLegacyIndexInfoVersion: legacyEncodingIndexVersionV1}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V1,
	// as well as the fields that were added in versions after V2,
	// and then restore them at the end of the test - This is required
	// because the new decoder won't try and read the new fields from
	// the old file format
	var (
		currSnapshotTime = testIndexInfo.SnapshotTime
		currFileType     = testIndexInfo.FileType
		currSnapshotID   = testIndexInfo.SnapshotID
	)
	testIndexInfo.SnapshotTime = 0
	testIndexInfo.FileType = 0
	testIndexInfo.SnapshotID = nil
	defer func() {
		testIndexInfo.SnapshotTime = currSnapshotTime
		testIndexInfo.FileType = currFileType
		testIndexInfo.SnapshotID = currSnapshotID
	}()

	enc.EncodeIndexInfo(testIndexInfo)
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V1 decoder code can handle the V3 file format.
func TestIndexInfoRoundTripForwardsCompatibilityV2(t *testing.T) {
	var (
		opts = legacyEncodingOptions{decodeLegacyIndexInfoVersion: legacyEncodingIndexVersionV1}
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
	)

	enc.EncodeIndexInfo(testIndexInfo)

	// Make sure to zero them before we compare, but after we have
	// encoded the data
	testIndexInfo.SnapshotTime = 0
	testIndexInfo.FileType = 0
	testIndexInfo.SnapshotID = nil
	defer func() {
		testIndexInfo.SnapshotTime = currSnapshotTime
		testIndexInfo.FileType = currFileType
		testIndexInfo.SnapshotID = currSnapshotID
	}()

	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V3 decoding code can handle the V2 file format.
func TestIndexInfoRoundTripBackwardsCompatibilityV2(t *testing.T) {
	var (
		opts = legacyEncodingOptions{encodeLegacyIndexInfoVersion: legacyEncodingIndexVersionV2}
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
	)
	testIndexInfo.SnapshotTime = 0
	testIndexInfo.FileType = 0
	testIndexInfo.SnapshotID = nil
	defer func() {
		testIndexInfo.SnapshotTime = currSnapshotTime
		testIndexInfo.FileType = currFileType
		testIndexInfo.SnapshotID = currSnapshotID
	}()

	enc.EncodeIndexInfo(testIndexInfo)
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

// Make sure the V2 decoder code can handle the V3 file format.
func TestIndexInfoRoundTripForwardsCompatibilityV3(t *testing.T) {
	var (
		opts = legacyEncodingOptions{decodeLegacyIndexInfoVersion: legacyEncodingIndexVersionV2}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V2
	// and then restore them at the end of the test - This is required
	// because the old decoder won't read the new fields.
	currSnapshotID := testIndexInfo.SnapshotID

	enc.EncodeIndexInfo(testIndexInfo)

	// Make sure to zero them before we compare, but after we have
	// encoded the data.
	testIndexInfo.SnapshotID = nil
	defer func() {
		testIndexInfo.SnapshotID = currSnapshotID
	}()

	dec.Reset(NewDecoderStream(enc.Bytes()))
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
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry()
	require.NoError(t, err)
	require.Equal(t, testIndexEntry, res)
}

// Make sure the V2 decoding code can handle the V1 file format.
func TestIndexEntryRoundTripBackwardsCompatibilityV1(t *testing.T) {
	var (
		opts = legacyEncodingOptions{encodeLegacyV1IndexEntry: true}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V1
	// and then restore them at the end of the test - This is required
	// because the new decoder won't try and read the new fields from
	// the old file format.
	currEncodedTags := testIndexEntry.EncodedTags
	testIndexEntry.EncodedTags = nil
	defer func() {
		testIndexEntry.EncodedTags = currEncodedTags
	}()

	enc.EncodeIndexEntry(testIndexEntry)
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry()
	require.NoError(t, err)
	require.Equal(t, testIndexEntry, res)
}

// Make sure the V1 decoder code can handle the V2 file format.
func TestIndexEntryRoundTripForwardsCompatibilityV2(t *testing.T) {
	var (
		opts = legacyEncodingOptions{decodeLegacyV1IndexEntry: true}
		enc  = newEncoder(opts)
		dec  = newDecoder(opts, nil)
	)

	// Set the default values on the fields that did not exist in V1
	// and then restore them at the end of the test - This is required
	// because the old decoder won't read the new fields.
	currEncodedTags := testIndexEntry.EncodedTags

	enc.EncodeIndexEntry(testIndexEntry)

	// Make sure to zero them before we compare, but after we have
	// encoded the data.
	testIndexEntry.EncodedTags = nil
	defer func() {
		testIndexEntry.EncodedTags = currEncodedTags
	}()

	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry()
	require.NoError(t, err)
	require.Equal(t, testIndexEntry, res)
}

func TestIndexSummaryRoundtrip(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeIndexSummary(testIndexSummary))
	dec.Reset(NewDecoderStream(enc.Bytes()))
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
	dec.Reset(NewDecoderStream(enc.Bytes()))
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
	dec.Reset(NewDecoderStream(enc.Bytes()))
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
	dec.Reset(NewDecoderStream(enc.Bytes()))
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
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeLogMetadata()
	require.NoError(t, err)
	require.Equal(t, testLogMetadata, res)
}

func TestMultiTypeRoundtripStress(t *testing.T) {
	var (
		enc    = NewEncoder()
		dec    = NewDecoder(nil)
		iter   = 10000
		res    interface{}
		err    error
		input  []interface{}
		output []interface{}
	)
	for i := 0; i < iter; i++ {
		switch i % 5 {
		case 0:
			require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
			input = append(input, testIndexInfo)
		case 1:
			require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
			input = append(input, testIndexEntry)
		case 2:
			require.NoError(t, enc.EncodeLogInfo(testLogInfo))
			input = append(input, testLogInfo)
		case 3:
			require.NoError(t, enc.EncodeLogEntry(testLogEntry))
			input = append(input, testLogEntry)
		case 4:
			require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
			input = append(input, testLogMetadata)
		}
	}

	dec.Reset(NewDecoderStream(enc.Bytes()))
	for i := 0; i < iter; i++ {
		switch i % 5 {
		case 0:
			res, err = dec.DecodeIndexInfo()
		case 1:
			res, err = dec.DecodeIndexEntry()
		case 2:
			res, err = dec.DecodeLogInfo()
		case 3:
			res, err = dec.DecodeLogEntry()
		case 4:
			res, err = dec.DecodeLogMetadata()
		}
		require.NoError(t, err)
		output = append(output, res)
	}
	require.Equal(t, input, output)
}
