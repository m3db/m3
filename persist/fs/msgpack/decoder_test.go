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

	"github.com/stretchr/testify/require"
)

func testGenEncodeNumObjectFieldsForFn(
	enc *Encoder,
	targetType objectType,
	delta int,
) encodeNumObjectFieldsForFn {
	return func(objType objectType) {
		if objType == targetType {
			_, curr := numFieldsForType(objType)
			enc.encodeArrayLenFn(curr + delta)
			return
		}
		enc.encodeNumObjectFieldsFor(objType)
	}
}

func TestDecodeNewerVersionThanExpected(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	// Intentionally bump client-side version
	enc.encodeVersionFn = func(version int) {
		enc.encodeVersion(version + 1)
	}

	// Verify decoding index info results in an error
	require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
	dec.Reset(NewDecoderStream(enc.Bytes()))
	_, err := dec.DecodeIndexInfo()
	require.Error(t, err)

	// Verify decoding index entry results in an error
	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
	dec.Reset(NewDecoderStream(enc.Bytes()))
	_, err = dec.DecodeIndexEntry()
	require.Error(t, err)

	// Verify decoding log info results in an error
	require.NoError(t, enc.EncodeLogInfo(testLogInfo))
	dec.Reset(NewDecoderStream(enc.Bytes()))
	_, err = dec.DecodeLogInfo()
	require.Error(t, err)

	// Verify decoding log entry results in an error
	require.NoError(t, enc.EncodeLogEntry(testLogEntry))
	dec.Reset(NewDecoderStream(enc.Bytes()))
	_, err = dec.DecodeLogEntry()
	require.Error(t, err)

	// Verify decoding log metadata results in an error
	require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
	dec.Reset(NewDecoderStream(enc.Bytes()))
	_, err = dec.DecodeLogMetadata()
	require.Error(t, err)
}

func TestDecodeRootObjectMoreFieldsThanExpected(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	// Intentionally bump number of fields for the root object
	enc.encodeNumObjectFieldsForFn = testGenEncodeNumObjectFieldsForFn(enc, rootObjectType, 1)
	require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
	require.NoError(t, enc.enc.EncodeInt64(1234))

	// Verify we can successfully skip unnecessary fields
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

func TestDecodeIndexInfoMoreFieldsThanExpected(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	// Intentionally bump number of fields for the index info object
	enc.encodeNumObjectFieldsForFn = testGenEncodeNumObjectFieldsForFn(enc, indexInfoType, 1)
	require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
	require.NoError(t, enc.enc.EncodeInt64(1234))

	// Verify we can successfully skip unnecessary fields
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

func TestDecodeIndexEntryMoreFieldsThanExpected(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	// Intentionally bump number of fields for the index entry object
	enc.encodeNumObjectFieldsForFn = testGenEncodeNumObjectFieldsForFn(enc, indexEntryType, 1)
	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
	require.NoError(t, enc.enc.EncodeInt64(1234))

	// Verify we can successfully skip unnecessary fields
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry()
	require.NoError(t, err)
	require.Equal(t, testIndexEntry, res)
}

func TestDecodeLogInfoMoreFieldsThanExpected(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	// Intentionally bump number of fields for the log info object
	enc.encodeNumObjectFieldsForFn = testGenEncodeNumObjectFieldsForFn(enc, logInfoType, 1)
	require.NoError(t, enc.EncodeLogInfo(testLogInfo))
	require.NoError(t, enc.enc.EncodeInt64(1234))

	// Verify we can successfully skip unnecessary fields
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeLogInfo()
	require.NoError(t, err)
	require.Equal(t, testLogInfo, res)
}

func TestDecodeLogEntryMoreFieldsThanExpected(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	// Intentionally bump number of fields for the log entry object
	enc.encodeNumObjectFieldsForFn = testGenEncodeNumObjectFieldsForFn(enc, logEntryType, 1)
	require.NoError(t, enc.EncodeLogEntry(testLogEntry))
	require.NoError(t, enc.enc.EncodeInt64(1234))

	// Verify we can successfully skip unnecessary fields
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeLogEntry()
	require.NoError(t, err)
	require.Equal(t, testLogEntry, res)
}

func TestDecodeLogMetadataMoreFieldsThanExpected(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	// Intentionally bump number of fields for the log metadata object
	enc.encodeNumObjectFieldsForFn = testGenEncodeNumObjectFieldsForFn(enc, logMetadataType, 1)
	require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
	require.NoError(t, enc.enc.EncodeInt64(1234))

	// Verify we can successfully skip unnecessary fields
	dec.Reset(NewDecoderStream(enc.Bytes()))
	res, err := dec.DecodeLogMetadata()
	require.NoError(t, err)
	require.Equal(t, testLogMetadata, res)
}

func TestDecodeLogEntryFewerFieldsThanExpected(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	// Intentionally bump number of fields for the log entry object
	enc.encodeNumObjectFieldsForFn = testGenEncodeNumObjectFieldsForFn(enc, logEntryType, -1)
	require.NoError(t, enc.EncodeLogEntry(testLogEntry))

	// Verify we can successfully skip unnecessary fields
	dec.Reset(NewDecoderStream(enc.Bytes()))
	_, err := dec.DecodeLogEntry()
	require.Error(t, err)
}

func TestDecodeBytesNoAlloc(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)

	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
	data := enc.Bytes()
	dec.Reset(NewDecoderStream(data))
	res, err := dec.DecodeIndexEntry()
	require.NoError(t, err)
	require.Equal(t, []byte("testIndexEntry"), res.ID)

	// Verify ID points to part of encoded data stream
	for i := 0; i < len(data); i++ {
		data[i] = byte('a')
	}
	require.Equal(t, []byte("aaaaaaaaaaaaaa"), res.ID)
}

func TestDecodeBytesAllocNew(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(NewDecodingOptions().SetAllocDecodedBytes(true))
	)

	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
	data := enc.Bytes()
	dec.Reset(NewDecoderStream(data))
	res, err := dec.DecodeIndexEntry()
	require.NoError(t, err)
	require.Equal(t, []byte("testIndexEntry"), res.ID)

	// Verify ID is not part of the encoded byte stream
	for i := 0; i < len(data); i++ {
		data[i] = byte('a')
	}
	require.Equal(t, []byte("testIndexEntry"), res.ID)
}
