// Copyright (c) 2018 Uber Technologies, Inc.
//
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

package proto

import (
	"net"
	"testing"

	"github.com/m3db/m3msg/generated/proto/msgpb"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecoderReset(t *testing.T) {
	c := NewEncodeDecoder(nil, nil).(*encdec)
	require.Nil(t, c.enc.w)
	require.Nil(t, c.dec.r)
	c.Close()
	// Safe to close again.
	c.Close()
	require.True(t, c.isClosed)

	conn := new(net.TCPConn)
	c.Reset(conn)
	require.False(t, c.isClosed)
	require.Equal(t, conn, c.enc.w)
	require.Equal(t, conn, c.dec.r)
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	c := NewEncodeDecoder(
		nil,
		NewEncodeDecoderOptions(),
	).(*encdec)

	clientConn, serverConn := net.Pipe()
	c.enc.resetWriter(clientConn)
	c.dec.resetReader(serverConn)

	testMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 10),
	}
	go func() {
		require.NoError(t, c.Encode(&testMsg))
	}()
	var msg msgpb.Message
	require.NoError(t, c.Decode(&msg))
	require.Equal(t, testMsg, msg)
}
