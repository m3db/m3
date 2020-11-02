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

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/x/pool"
	xtest "github.com/m3db/m3/src/x/test"
	xhash "github.com/m3db/m3/src/x/test/hash"

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
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	_, err := dec.DecodeIndexInfo()
	require.Error(t, err)

	// Verify decoding index entry results in an error
	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	_, err = dec.DecodeIndexEntry(nil)
	require.Error(t, err)

	// Verify decoding log info results in an error
	require.NoError(t, enc.EncodeLogInfo(testLogInfo))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	_, err = dec.DecodeLogInfo()
	require.Error(t, err)

	// Verify decoding log entry results in an error
	require.NoError(t, enc.EncodeLogEntry(testLogEntry))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	_, err = dec.DecodeLogEntry()
	require.Error(t, err)

	// Verify decoding log metadata results in an error
	require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
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
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
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
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
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

	// This hokey bit of logic is done so we can add extra fields in the correct location (since new IndexEntry fields
	// will be added *before* the checksum). Confirm current checksum is correct, strip it, add unexpected field,
	// and re-add updated checksum value

	// Validate existing checksum
	checksumPos := len(enc.Bytes()) - 5 // 5 bytes = 1 byte for integer code + 4 bytes for checksum
	dec.Reset(NewByteDecoderStream(enc.Bytes()[checksumPos:]))
	currChecksum := dec.decodeVarint()
	require.Equal(t, currChecksum, int64(digest.Checksum(enc.Bytes()[:checksumPos])))

	// Strip checksum, add new field, add updated checksum
	enc.buf.Truncate(len(enc.Bytes()) - 5)
	require.NoError(t, enc.enc.EncodeInt64(1234))
	checksum := int64(digest.Checksum(enc.Bytes()))
	require.NoError(t, enc.enc.EncodeInt64(checksum))
	expected := testIndexEntry
	expected.IndexChecksum = checksum

	// Verify we can successfully skip unnecessary fields
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	res, err := dec.DecodeIndexEntry(nil)
	require.NoError(t, err)
	require.Equal(t, expected, res)
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
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
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
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
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
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
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
	dec.Reset(NewByteDecoderStream(enc.Bytes()))
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
	dec.Reset(NewByteDecoderStream(data))
	res, err := dec.DecodeIndexEntry(nil)
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
	dec.Reset(NewByteDecoderStream(data))
	res, err := dec.DecodeIndexEntry(nil)
	require.NoError(t, err)
	require.Equal(t, []byte("testIndexEntry"), res.ID)

	// Verify ID is not part of the encoded byte stream
	for i := 0; i < len(data); i++ {
		data[i] = byte('a')
	}
	require.Equal(t, []byte("testIndexEntry"), res.ID)
}

func TestDecodeIndexEntryInvalidWideEntry(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))

	// Update to invalid checksum
	enc.buf.Truncate(len(enc.Bytes()) - 5) // 5 bytes = 1 byte for integer code + 4 bytes for checksum
	require.NoError(t, enc.enc.EncodeInt64(1234))

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	_, err := dec.DecodeIndexEntry(nil)
	require.EqualError(t, err, errorIndexEntryChecksumMismatch.Error())
}

func TestDecodeIndexEntryIncompleteFile(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(nil)
	)
	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))

	enc.buf.Truncate(len(enc.Bytes()) - 4)

	dec.Reset(NewByteDecoderStream(enc.Bytes()))
	_, err := dec.DecodeIndexEntry(nil)
	require.EqualError(t, err, "decode index entry encountered error: EOF")
}

var decodeWideEntryTests = []struct {
	id         string
	exStatus   WideEntryLookupStatus
	exChecksum int64
}{
	{id: "aaa", exStatus: NotFoundLookupStatus},
	{id: "test100", exStatus: MatchedLookupStatus, exChecksum: testMetadataChecksum},
	{id: "zzz", exStatus: MismatchLookupStatus},
}

func TestDecodeToWideEntry(t *testing.T) {
	var (
		enc = NewEncoder()
		dec = NewDecoder(NewDecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t)))
	)

	require.NoError(t, enc.EncodeIndexEntry(testWideEntry.IndexEntry))
	data := enc.Bytes()

	for _, tt := range decodeWideEntryTests {
		t.Run(tt.id, func(t *testing.T) {
			dec.Reset(NewByteDecoderStream(data))
			res, status, err := dec.DecodeToWideEntry([]byte(tt.id), nil)
			require.NoError(t, err)
			require.Equal(t, tt.exStatus, status)
			if tt.exStatus == MatchedLookupStatus {
				require.Equal(t, tt.exChecksum, res.MetadataChecksum)
			}
		})
	}
}

func TestDecodeToWideEntryPooled(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		enc = NewEncoder()
		dec = NewDecoder(NewDecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t)))
	)

	require.NoError(t, enc.EncodeIndexEntry(testWideEntry.IndexEntry))
	data := enc.Bytes()

	for _, tt := range decodeWideEntryTests {
		t.Run(tt.id+"_pooled", func(t *testing.T) {
			dec.Reset(NewByteDecoderStream(data))

			bytePool := pool.NewMockBytesPool(ctrl)
			idLength := len(testWideEntry.ID)
			idBytes := make([]byte, idLength)
			bytePool.EXPECT().Get(idLength).Return(idBytes)

			tagLength := len(testWideEntry.EncodedTags)
			tagBytes := make([]byte, tagLength)
			bytePool.EXPECT().Get(tagLength).Return(tagBytes)

			if tt.exStatus != MatchedLookupStatus {
				bytePool.EXPECT().Put(idBytes)
				bytePool.EXPECT().Put(tagBytes)
			}

			res, status, err := dec.DecodeToWideEntry([]byte(tt.id), bytePool)
			require.NoError(t, err)
			require.Equal(t, tt.exStatus, status)
			if tt.exStatus == MatchedLookupStatus {
				require.Equal(t, tt.exChecksum, res.MetadataChecksum)
			}
		})
	}
}
