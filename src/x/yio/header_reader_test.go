package yio

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeaderReader(t *testing.T) {
	headerStrData := "header"
	strData := "foobarbaz"
	for size := 1; size < len(headerStrData)+len(strData)+5; size++ {
		var (
			headerData = []byte(headerStrData)
			data       = []byte(strData)
			buf        = bytes.NewBuffer(data)
			reader     = newHeaderReader(headerData, buf)

			str = ""

			n   int
			err error
		)
		for i := 0; i < 20 && err == nil; i++ {
			p := make([]byte, size)
			n, err = reader.Read(p)
			assert.True(t, n <= size)
			str = str + string(p[:len(p)])
		}
		assert.Equal(t, io.EOF, err)
		assert.Equal(t, "headerfoobarbaz", strings.TrimRight(str, string([]byte{0})))
	}
}

func TestHeaderReaderReset(t *testing.T) {
	buf := bytes.NewBuffer([]byte("foobarbaz"))
	reader := newHeaderReader([]byte("header"), buf)
	read, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, "headerfoobarbaz", string(read))
	headerReader := reader.(*headerReader)
	resetBuf := bytes.NewBuffer([]byte(" new reader"))
	headerReader.reset([]byte("new header"), resetBuf)
	resetResult, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, "new header new reader", string(resetResult))
}

func TestWrite(t *testing.T) {
	var bs []byte
	buf := bytes.NewBuffer(bs)
	n, err := buf.Write([]byte("foobarbazbiz"))
	require.NoError(t, err)
	assert.Equal(t, len("foobarbazbiz"), n)
	fmt.Println("bs", string(buf.Bytes()), "!", buf.Bytes())
	buf.Reset()
	buf.Write([]byte("cake"))
	fmt.Println("bs", string(buf.Bytes()), "!", buf.Bytes())
}
