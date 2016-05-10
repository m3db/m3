// Copyright (c) 2015 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tchannel

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/testutils/testreader"
	"github.com/uber/tchannel-go/typed"
)

func TestFrameHeaderJSON(t *testing.T) {
	fh := FrameHeader{
		size:        uint16(0xFF34),
		messageType: messageTypeCallReq,
		ID:          0xDEADBEEF,
	}
	logged, err := json.Marshal(fh)
	assert.NoError(t, err, "FrameHeader can't be marshalled to JSON")
	assert.Equal(
		t,
		string(logged),
		`{"id":3735928559,"msgType":3,"size":65332}`,
		"FrameHeader didn't marshal to JSON as expected",
	)
}

func TestFraming(t *testing.T) {
	fh := FrameHeader{
		size:        uint16(0xFF34),
		messageType: messageTypeCallReq,
		ID:          0xDEADBEEF,
	}

	wbuf := typed.NewWriteBufferWithSize(1024)
	require.Nil(t, fh.write(wbuf))

	var b bytes.Buffer
	if _, err := wbuf.FlushTo(&b); err != nil {
		require.Nil(t, err)
	}

	rbuf := typed.NewReadBuffer(b.Bytes())

	var fh2 FrameHeader
	require.Nil(t, fh2.read(rbuf))

	assert.Equal(t, fh, fh2)
}

func TestPartialRead(t *testing.T) {
	f := NewFrame(MaxFramePayloadSize)
	f.Header.size = FrameHeaderSize + 2134
	f.Header.messageType = messageTypeCallReq
	f.Header.ID = 0xDEADBEED

	// We set the full payload but only the first 2134 bytes should be written.
	for i := 0; i < len(f.Payload); i++ {
		val := (i * 37) % 256
		f.Payload[i] = byte(val)
	}
	buf := &bytes.Buffer{}
	require.NoError(t, f.WriteOut(buf))
	assert.Equal(t, f.Header.size, uint16(buf.Len()), "frame size should match written bytes")

	// Read the data back, from a reader that fragments.
	f2 := NewFrame(MaxFramePayloadSize)
	require.NoError(t, f2.ReadIn(iotest.OneByteReader(buf)))

	// Ensure header and payload are the same.
	require.Equal(t, f.Header, f2.Header, "frame headers don't match")
	require.Equal(t, f.SizedPayload(), f2.SizedPayload(), "payload does not match")
}

func TestEmptyPayload(t *testing.T) {
	f := NewFrame(MaxFramePayloadSize)
	m := &pingRes{id: 1}
	require.NoError(t, f.write(m))

	// Write out the frame.
	buf := &bytes.Buffer{}
	require.NoError(t, f.WriteOut(buf))
	assert.Equal(t, FrameHeaderSize, buf.Len())

	// Read the frame from the buffer.
	// net.Conn returns io.EOF if you try to read 0 bytes at the end.
	// This is also simulated by the LimitedReader so we use that here.
	require.NoError(t, f.ReadIn(&io.LimitedReader{R: buf, N: FrameHeaderSize}))
}

func TestReservedBytes(t *testing.T) {
	// Set up a frame with non-zero values
	f := NewFrame(MaxFramePayloadSize)
	reader := testreader.Looper([]byte{^byte(0)})
	io.ReadFull(reader, f.Payload)
	f.Header.read(typed.NewReadBuffer(f.Payload))

	m := &pingRes{id: 1}
	f.write(m)

	buf := &bytes.Buffer{}
	f.WriteOut(buf)
	assert.Equal(t,
		[]byte{
			0x0, 0x10, // size
			0xd1,               // type
			0x0,                // reserved should always be 0
			0x0, 0x0, 0x0, 0x1, // id
			0x0, 0x0, 0x0, 0x0, // reserved should always be 0
			0x0, 0x0, 0x0, 0x0, // reserved should always be 0
		},
		buf.Bytes(), "Unexpected bytes")
}
