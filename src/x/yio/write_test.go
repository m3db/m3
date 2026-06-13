package yio

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var databytes = []byte{10, 4, 8, 116, 16, 13, 18, 145, 1, 8, 6, 58, 140, 1, 10, 98, 8, 3, 18, 75, 117, 39, 4, 0, 6, 0, 95, 95, 103, 48, 95, 95, 3, 0, 102, 111, 111, 6, 0, 95, 95, 103, 49, 95, 95, 3, 0, 98, 97, 114, 6, 0, 95, 95, 103, 50, 95, 95, 3, 0, 98, 97, 122, 20, 0, 95, 95, 111, 112, 116, 105, 111, 110, 95, 105, 100, 95, 115, 99, 104, 101, 109, 101, 95, 95, 8, 0, 103, 114, 97, 112, 104, 105, 116, 101, 24, 128, 228, 222, 128, 248, 241, 213, 142, 22, 33, 0, 0, 0, 0, 0, 0, 68, 64, 18, 38, 10, 36, 26, 34, 10, 32, 10, 2, 8, 16, 18, 24, 10, 12, 8, 128, 200, 175, 160, 37, 16, 128, 148, 235, 220, 3, 18, 8, 8, 128, 128, 207, 162, 210, 244, 4, 26, 0}

func TestWriteBytes(t *testing.T) {
	// fmt.Println("\n\x04\bt\x10\r\x12\x91\x01\b\x06:\x8c\x01\nb\b\x03\x12Ku'\x04\x00\x06\x00__g0__\x03\x00foo\x06\x00__g1__\x03\x00bar\x06\x00__g2__\x03\x00baz\x14\x00__option_id_scheme__\b\x00graphite\x18\x80\xe4ހ\xf8\xf1Վ\x16!\x00\x00\x00\x00\x00\x00D@\x12&\n$\x1a\"\n \n\x02\b\x10\x12\x18\n\f\b\x80ȯ\xa0%\x10\x80\x94\xeb\xdc\x03\x12\b\b\x80\x80Ϣ\xd2\xf4\x04\x1a\x00")
	// assert.Equal(t, "", string(databytes))
	buf := new(bytes.Buffer)
	writer := snappy.NewWriter(buf)
	bs, err := writer.Write(databytes)
	require.NoError(t, err)
	// fmt.Println("Byte buffer before", buf.Bytes())
	require.NoError(t, writer.Flush())
	fmt.Println("Byte buffer after flush", buf.Bytes())
	assert.Equal(t, len(databytes), bs)
}

func TestPrint(t *testing.T) {
	fmt.Println(string([]byte{115, 78, 97, 80, 112, 89, 1}))
}
