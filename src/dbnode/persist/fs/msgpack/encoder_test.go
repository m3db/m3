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
	"errors"
	"testing"

	"github.com/m3db/m3/src/dbnode/persist/schema"

	"github.com/stretchr/testify/require"
)

var (
	errTestVarint   = errors.New("test varint error")
	errTestFloat64  = errors.New("test float64 error")
	errTestBytes    = errors.New("test bytes error")
	errTestArrayLen = errors.New("test array len error")
)

func testCapturingEncoder(t *testing.T) (*Encoder, *[]interface{}) {
	encoder := NewEncoder()

	var result []interface{}
	actualEncodeVarintFn := encoder.encodeVarintFn
	encoder.encodeVarintFn = func(value int64) {
		actualEncodeVarintFn(value)
		result = append(result, value)
	}

	actualEncodeVarUintFn := encoder.encodeVarUintFn
	encoder.encodeVarUintFn = func(value uint64) {
		actualEncodeVarUintFn(value)
		result = append(result, value)
	}

	actualEncodeFloat64Fn := encoder.encodeFloat64Fn
	encoder.encodeFloat64Fn = func(value float64) {
		actualEncodeFloat64Fn(value)
		result = append(result, value)
	}

	actualEncodeBytesFn := encoder.encodeBytesFn
	encoder.encodeBytesFn = func(value []byte) {
		actualEncodeBytesFn(value)
		result = append(result, value)
	}

	actualEncodeArrayLenFn := encoder.encodeArrayLenFn
	encoder.encodeArrayLenFn = func(value int) {
		actualEncodeArrayLenFn(value)
		result = append(result, value)
	}

	return encoder, &result
}

func testExpectedResultForIndexInfo(t *testing.T, indexInfo schema.IndexInfo) []interface{} {
	_, currRoot := numFieldsForType(rootObjectType)
	_, currIndexInfo := numFieldsForType(indexInfoType)
	_, currSummariesInfo := numFieldsForType(indexSummariesInfoType)
	_, currIndexBloomFilterInfo := numFieldsForType(indexBloomFilterInfoType)
	return []interface{}{
		int64(indexInfoVersion),
		currRoot,
		int64(indexInfoType),
		currIndexInfo,
		indexInfo.BlockStart,
		indexInfo.BlockSize,
		indexInfo.Entries,
		indexInfo.MajorVersion,
		currSummariesInfo,
		indexInfo.Summaries.Summaries,
		currIndexBloomFilterInfo,
		indexInfo.BloomFilter.NumElementsM,
		indexInfo.BloomFilter.NumHashesK,
		indexInfo.SnapshotTime,
		int64(indexInfo.FileType),
		indexInfo.SnapshotID,
		int64(indexInfo.VolumeIndex),
		indexInfo.MinorVersion,
	}
}

func testExpectedResultForIndexEntry(t *testing.T, indexEntry schema.IndexEntry) []interface{} {
	_, currRoot := numFieldsForType(rootObjectType)
	_, currIndexEntry := numFieldsForType(indexEntryType)
	return []interface{}{
		int64(indexEntryVersion),
		currRoot,
		int64(indexEntryType),
		currIndexEntry,
		indexEntry.Index,
		indexEntry.ID,
		indexEntry.Size,
		indexEntry.Offset,
		indexEntry.DataChecksum,
		indexEntry.EncodedTags,
		int64(testIndexEntryChecksum), // Checksum auto-added to the end of the index entry
	}
}

func testExpectedResultForLogInfo(t *testing.T, logInfo schema.LogInfo) []interface{} {
	_, currRoot := numFieldsForType(rootObjectType)
	_, currLogInfo := numFieldsForType(logInfoType)
	return []interface{}{
		int64(logInfoVersion),
		currRoot,
		int64(logInfoType),
		currLogInfo,
		logInfo.DeprecatedDoNotUseStart,
		logInfo.DeprecatedDoNotUseDuration,
		logInfo.Index,
	}
}

func testExpectedResultForLogEntry(t *testing.T, logEntry schema.LogEntry) []interface{} {
	_, currRoot := numFieldsForType(rootObjectType)
	_, currLogEntry := numFieldsForType(logEntryType)
	return []interface{}{
		int64(logEntryVersion),
		currRoot,
		int64(logEntryType),
		currLogEntry,
		logEntry.Index,
		logEntry.Create,
		logEntry.Metadata,
		logEntry.Timestamp,
		logEntry.Value,
		uint64(logEntry.Unit),
		logEntry.Annotation,
	}
}

func testExpectedResultForLogMetadata(t *testing.T, logMetadata schema.LogMetadata) []interface{} {
	_, currRoot := numFieldsForType(rootObjectType)
	_, currLogMetadata := numFieldsForType(logMetadataType)
	return []interface{}{
		int64(logMetadataVersion),
		currRoot,
		int64(logMetadataType),
		currLogMetadata,
		logMetadata.ID,
		logMetadata.Namespace,
		uint64(logMetadata.Shard),
		logMetadata.EncodedTags,
	}
}

func TestEncodeIndexInfo(t *testing.T) {
	enc, actual := testCapturingEncoder(t)
	require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
	expected := testExpectedResultForIndexInfo(t, testIndexInfo)
	require.Equal(t, expected, *actual)
}

func TestEncodeIndexEntry(t *testing.T) {
	enc, actual := testCapturingEncoder(t)
	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
	expected := testExpectedResultForIndexEntry(t, testIndexEntry)
	require.Equal(t, expected, *actual)
}

func TestEncodeLogInfo(t *testing.T) {
	enc, actual := testCapturingEncoder(t)
	require.NoError(t, enc.EncodeLogInfo(testLogInfo))
	expected := testExpectedResultForLogInfo(t, testLogInfo)
	require.Equal(t, expected, *actual)
}

func TestEncodeLogEntry(t *testing.T) {
	enc, actual := testCapturingEncoder(t)
	require.NoError(t, enc.EncodeLogEntry(testLogEntry))
	expected := testExpectedResultForLogEntry(t, testLogEntry)
	require.Equal(t, expected, *actual)
}

func TestEncodeLogEntryFast(t *testing.T) {
	buffer, err := EncodeLogEntryFast(nil, testLogEntry)
	require.NoError(t, err)

	enc := NewEncoder()
	enc.EncodeLogEntry(testLogEntry)
	expected := enc.Bytes()

	require.Equal(t, expected, buffer)
}

func TestEncodeLogMetadata(t *testing.T) {
	enc, actual := testCapturingEncoder(t)
	require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
	expected := testExpectedResultForLogMetadata(t, testLogMetadata)
	require.Equal(t, expected, *actual)
}

func TestEncodeLogMetadataFast(t *testing.T) {
	buffer, err := EncodeLogMetadataFast(nil, testLogMetadata)
	require.NoError(t, err)

	enc := NewEncoder()
	enc.EncodeLogMetadata(testLogMetadata)
	expected := enc.Bytes()

	require.Equal(t, expected, buffer)
}

func TestEncodeVarintError(t *testing.T) {
	enc, _ := testCapturingEncoder(t)
	enc.encodeVarintFn = func(value int64) { enc.err = errTestVarint }
	require.Equal(t, errTestVarint, enc.EncodeIndexInfo(testIndexInfo))
}

func TestEncodeFloat64Error(t *testing.T) {
	enc, _ := testCapturingEncoder(t)
	enc.encodeFloat64Fn = func(value float64) { enc.err = errTestFloat64 }
	require.Equal(t, errTestFloat64, enc.EncodeLogEntry(testLogEntry))
}

func TestEncodeBytesError(t *testing.T) {
	enc, _ := testCapturingEncoder(t)
	enc.encodeBytesFn = func(value []byte) { enc.err = errTestBytes }
	require.Equal(t, errTestBytes, enc.EncodeIndexEntry(testIndexEntry))
}

func TestEncodeArrayLenError(t *testing.T) {
	enc, _ := testCapturingEncoder(t)
	enc.encodeArrayLenFn = func(value int) { enc.err = errTestArrayLen }
	require.Equal(t, errTestArrayLen, enc.EncodeLogInfo(testLogInfo))
}
