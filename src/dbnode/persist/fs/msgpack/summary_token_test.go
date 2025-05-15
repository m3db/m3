package msgpack

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/vmihailenco/msgpack.v2"
)

func TestIndexSummaryToken(t *testing.T) {
	idBytes := []byte("my-test-id")
	indexOffset := int64(123456789)

	var indexOffsetBuf bytes.Buffer
	encoder := msgpack.NewEncoder(&indexOffsetBuf)
	err := encoder.Encode(indexOffset)
	require.NoError(t, err)

	fullBuffer := idBytes
	fullBuffer = append(fullBuffer, indexOffsetBuf.Bytes()...)

	token := NewIndexSummaryToken(0, uint32(len(idBytes)))

	returnedID := token.ID(fullBuffer)
	require.Equal(t, idBytes, returnedID)

	stream := NewByteDecoderStream(idBytes)
	reader := bytes.NewReader(idBytes)
	msgpackDecoder := msgpack.NewDecoder(reader)

	returnedOffset, err := token.IndexOffset(fullBuffer, stream, msgpackDecoder)
	require.NoError(t, err)
	require.Equal(t, indexOffset, returnedOffset)
}
