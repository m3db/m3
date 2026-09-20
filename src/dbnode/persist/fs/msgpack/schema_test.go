// Copyright (c) 2026 Uber Technologies, Inc
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

	"github.com/m3db/m3/src/dbnode/persist/schema"
)

// The commit log headers are built at init time by the msgpack library encoder, which sizes
// each integer down to the smallest encoding it fits in. DecodeLogEntryFast skips the header
// by length alone, without validating its contents, so a msgpack version that widens integers
// would grow these headers and make new binaries silently misparse commit logs written by old
// ones. Pin the bytes so that such an upgrade fails here instead of during bootstrap. See the
// note on the msgpack replace directive in go.mod.
func TestCommitLogHeadersUnchanged(t *testing.T) {
	require.NoError(t, logEntryHeaderErr)
	require.NoError(t, logMetadataHeaderErr)

	// Version 1, root object array len 2, logEntryType (8), log entry array len 7.
	require.Equal(t, []byte{0x01, 0x92, 0x08, 0x97}, logEntryHeader)
	// Version 1, root object array len 2, logMetadataType (9), log metadata array len 3.
	require.Equal(t, []byte{0x01, 0x92, 0x09, 0x93}, logMetadataHeader)
}

// The fixtures below are literal bytes rather than freshly encoded ones, so unlike the other
// fast path tests these do not re-encode and decode with the same build. They pin the whole
// on-disk layout, including the integer widths chosen for each field.
var (
	logEntryFixture = []byte{
		0x01, 0x92, 0x08, 0x97, // header
		0x7b,                                                 // Index 123
		0xcf, 0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x00, // Create
		0xc4, 0x04, 0x6d, 0x65, 0x74, 0x61, // Metadata "meta"
		0xcf, 0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x01, // Timestamp
		0xcb, 0x40, 0x09, 0x1e, 0xb8, 0x51, 0xeb, 0x85, 0x1f, // Value 3.14
		0x02,                                     // Unit 2
		0xc4, 0x05, 0x61, 0x6e, 0x6e, 0x6f, 0x74, // Annotation "annot"
	}

	logEntryFixtureDecoded = schema.LogEntry{
		Index:      123,
		Create:     1000000000000000000,
		Metadata:   []byte("meta"),
		Timestamp:  1000000000000000001,
		Value:      3.14,
		Unit:       2,
		Annotation: []byte("annot"),
	}

	logMetadataFixture = []byte{
		0x01, 0x92, 0x09, 0x93, // header
		0xc4, 0x02, 0x69, 0x64, // ID "id"
		0xc4, 0x02, 0x6e, 0x73, // Namespace "ns"
		0x0a,                               // Shard 10
		0xc4, 0x04, 0x74, 0x61, 0x67, 0x73, // EncodedTags "tags"
	}

	logMetadataFixtureDecoded = schema.LogMetadata{
		ID:          []byte("id"),
		Namespace:   []byte("ns"),
		Shard:       10,
		EncodedTags: []byte("tags"),
	}
)

func TestLogEntryFixtureRoundtrip(t *testing.T) {
	encoded, err := EncodeLogEntryFast(nil, logEntryFixtureDecoded)
	require.NoError(t, err)
	require.Equal(t, logEntryFixture, encoded)

	decoded, err := DecodeLogEntryFast(logEntryFixture)
	require.NoError(t, err)
	require.Equal(t, logEntryFixtureDecoded, decoded)
}

func TestLogMetadataFixtureRoundtrip(t *testing.T) {
	encoded, err := EncodeLogMetadataFast(nil, logMetadataFixtureDecoded)
	require.NoError(t, err)
	require.Equal(t, logMetadataFixture, encoded)

	decoded, err := DecodeLogMetadataFast(logMetadataFixture)
	require.NoError(t, err)
	require.Equal(t, logMetadataFixtureDecoded, decoded)
}
