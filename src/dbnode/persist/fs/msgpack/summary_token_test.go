package msgpack

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestIndexSummaryToken(t *testing.T) {
	// Original ID
	idBytes := []byte("my-test-id")

	// Index offset to be encoded and appended
	indexOffset := int64(123456789)

	// Encode the indexOffset using msgpack
	var indexOffsetBuf bytes.Buffer
	encoder := msgpack.NewEncoder(&indexOffsetBuf)
	err := encoder.Encode(indexOffset)
	require.NoError(t, err)

	// Combine ID and encoded offset
	fullBuffer := idBytes
	fullBuffer = append(fullBuffer, indexOffsetBuf.Bytes()...)

	// Create IndexSummaryToken pointing to the ID
	token := NewIndexSummaryToken(0, uint32(len(idBytes)))

	// Verify ID() returns correct ID slice
	returnedID := token.ID(fullBuffer)
	require.Equal(t, idBytes, returnedID)

	// Prepare decoder and stream
	stream := NewByteDecoderStream(idBytes)
	reader := bytes.NewReader(idBytes)
	msgpackDecoder := msgpack.NewDecoder(reader)

	// Decode index offset
	returnedOffset, err := token.IndexOffset(fullBuffer, stream, msgpackDecoder)
	require.NoError(t, err)
	require.Equal(t, indexOffset, returnedOffset)
}
