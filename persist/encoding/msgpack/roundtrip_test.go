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

	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/schema"

	"github.com/stretchr/testify/require"
)

var (
	testIndexInfo = schema.IndexInfo{
		Start:     time.Now().UnixNano(),
		BlockSize: int64(2 * time.Hour),
		Entries:   2000000,
	}

	testIndexEntry = schema.IndexEntry{
		Index:    234,
		ID:       []byte("testIndexEntry"),
		Size:     5456,
		Offset:   2390423,
		Checksum: 134245634534,
	}

	testLogInfo = schema.LogInfo{
		Start:    time.Now().UnixNano(),
		Duration: int64(2 * time.Hour),
		Index:    234,
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
		ID:        []byte("testLogMetadata"),
		Namespace: []byte("testNamespace"),
		Shard:     123,
	}
)

func testEncoder(t *testing.T) encoding.Encoder {
	return NewEncoder()
}

func testDecoder(t *testing.T, opts DecodingOptions) encoding.Decoder {
	return NewDecoder(opts)
}

func TestIndexInfoRoundtrip(t *testing.T) {
	var (
		enc = testEncoder(t)
		dec = testDecoder(t, nil)
	)
	require.NoError(t, enc.EncodeIndexInfo(testIndexInfo))
	dec.Reset(enc.Bytes())
	res, err := dec.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, testIndexInfo, res)
}

func TestIndexEntryRoundtrip(t *testing.T) {
	var (
		enc = testEncoder(t)
		dec = testDecoder(t, nil)
	)
	require.NoError(t, enc.EncodeIndexEntry(testIndexEntry))
	dec.Reset(enc.Bytes())
	res, err := dec.DecodeIndexEntry()
	require.NoError(t, err)
	require.Equal(t, testIndexEntry, res)
}

func TestLogInfoRoundtrip(t *testing.T) {
	var (
		enc = testEncoder(t)
		dec = testDecoder(t, nil)
	)
	require.NoError(t, enc.EncodeLogInfo(testLogInfo))
	dec.Reset(enc.Bytes())
	res, err := dec.DecodeLogInfo()
	require.NoError(t, err)
	require.Equal(t, testLogInfo, res)
}

func TestLogEntryRoundtrip(t *testing.T) {
	var (
		enc = testEncoder(t)
		dec = testDecoder(t, nil)
	)
	require.NoError(t, enc.EncodeLogEntry(testLogEntry))
	dec.Reset(enc.Bytes())
	res, err := dec.DecodeLogEntry()
	require.NoError(t, err)
	require.Equal(t, testLogEntry, res)
}

func TestLogMetadataRoundtrip(t *testing.T) {
	var (
		enc = testEncoder(t)
		dec = testDecoder(t, nil)
	)
	require.NoError(t, enc.EncodeLogMetadata(testLogMetadata))
	dec.Reset(enc.Bytes())
	res, err := dec.DecodeLogMetadata()
	require.NoError(t, err)
	require.Equal(t, testLogMetadata, res)
}

func TestMultiTypeRoundtripStress(t *testing.T) {
	var (
		enc    = testEncoder(t)
		dec    = testDecoder(t, nil)
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

	dec.Reset(enc.Bytes())
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
