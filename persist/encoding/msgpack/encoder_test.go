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

	"github.com/m3db/m3db/persist/schema"

	"github.com/stretchr/testify/require"
)

var (
	errTestVarint   = errors.New("test varint error")
	errTestFloat64  = errors.New("test float64 error")
	errTestBytes    = errors.New("test bytes error")
	errTestArrayLen = errors.New("test array len error")
)

func testCapturingEncoder(t *testing.T) (*encoder, *[]interface{}) {
	encoder := testEncoder(t).(*encoder)

	var result []interface{}
	encoder.encodeVarintFn = func(value int64) {
		result = append(result, value)
	}
	encoder.encodeVarUintFn = func(value uint64) {
		result = append(result, value)
	}
	encoder.encodeFloat64Fn = func(value float64) {
		result = append(result, value)
	}
	encoder.encodeBytesFn = func(value []byte) {
		result = append(result, value)
	}
	encoder.encodeArrayLenFn = func(value int) {
		result = append(result, value)
	}

	return encoder, &result
}

func testExpectedResultForIndexInfo(t *testing.T, indexInfo schema.IndexInfo) []interface{} {
	return []interface{}{
		int64(indexInfoVersion),
		numFieldsForType(rootObjectType),
		int64(indexInfoType),
		numFieldsForType(indexInfoType),
		indexInfo.Start,
		indexInfo.BlockSize,
		indexInfo.Entries,
	}
}

func testExpectedResultForIndexEntry(t *testing.T, indexEntry schema.IndexEntry) []interface{} {
	return []interface{}{
		int64(indexEntryVersion),
		numFieldsForType(rootObjectType),
		int64(indexEntryType),
		numFieldsForType(indexEntryType),
		indexEntry.Index,
		indexEntry.ID,
		indexEntry.Size,
		indexEntry.Offset,
		indexEntry.Checksum,
	}
}

func testExpectedResultForLogInfo(t *testing.T, logInfo schema.LogInfo) []interface{} {
	return []interface{}{
		int64(logInfoVersion),
		numFieldsForType(rootObjectType),
		int64(logInfoType),
		numFieldsForType(logInfoType),
		logInfo.Start,
		logInfo.Duration,
		logInfo.Index,
	}
}

func testExpectedResultForLogEntry(t *testing.T, logEntry schema.LogEntry) []interface{} {
	return []interface{}{
		int64(logEntryVersion),
		numFieldsForType(rootObjectType),
		int64(logEntryType),
		numFieldsForType(logEntryType),
		logEntry.Create,
		logEntry.Index,
		logEntry.Metadata,
		logEntry.Timestamp,
		logEntry.Value,
		uint64(logEntry.Unit),
		logEntry.Annotation,
	}
}

func testExpectedResultForLogMetadata(t *testing.T, logMetadata schema.LogMetadata) []interface{} {
	return []interface{}{
		int64(logMetadataVersion),
		numFieldsForType(rootObjectType),
		int64(logMetadataType),
		numFieldsForType(logMetadataType),
		logMetadata.ID,
		logMetadata.Namespace,
		uint64(logMetadata.Shard),
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

func TestEncodeLogMetadata(t *testing.T) {
	enc, actual := testCapturingEncoder(t)
	require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
	expected := testExpectedResultForLogMetadata(t, testLogMetadata)
	require.Equal(t, expected, *actual)
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
