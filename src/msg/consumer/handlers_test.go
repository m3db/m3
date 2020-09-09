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

package consumer

import (
	"net"
	"sync"
	"testing"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/server"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var testOpts = []Options{testOptions(), testOptionsUsingSnappyCompression()}

func TestServerWithMessageFn(t *testing.T) {
	defer leaktest.Check(t)()

	for _, opts := range testOpts {
		var (
			data []string
			wg   sync.WaitGroup
		)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		p := NewMockMessageProcessor(ctrl)
		p.EXPECT().Process(gomock.Any()).Do(
			func(m Message) {
				data = append(data, string(m.Bytes()))
				m.Ack()
				wg.Done()
			},
		).Times(2)
		// Set a large ack buffer size to make sure the background go routine
		// can flush it.
		opts = opts.SetAckBufferSize(100)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		s := server.NewServer("a", NewMessageHandler(p, opts), server.NewOptions())
		s.Serve(l)

		conn, err := net.Dial("tcp", l.Addr().String())
		require.NoError(t, err)

		wg.Add(1)
		err = produce(conn, &testMsg1, opts)
		require.NoError(t, err)
		wg.Add(1)
		err = produce(conn, &testMsg2, opts)
		require.NoError(t, err)

		wg.Wait()
		require.Equal(t, string(testMsg1.Value), data[0])
		require.Equal(t, string(testMsg2.Value), data[1])

		var ack msgpb.Ack
		testDecoder := proto.NewDecoder(conn, opts.DecoderOptions(), 10)
		err = testDecoder.Decode(&ack)
		require.NoError(t, err)
		require.Equal(t, 2, len(ack.Metadata))
		require.Equal(t, testMsg1.Metadata, ack.Metadata[0])
		require.Equal(t, testMsg2.Metadata, ack.Metadata[1])

		p.EXPECT().Close()
		s.Close()
	}
}

func TestServerWithConsumeFn(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		count  = 0
		bytes  []byte
		closed bool
		wg     sync.WaitGroup
	)
	consumeFn := func(c Consumer) {
		for {
			count++
			m, err := c.Message()
			if err != nil {
				break
			}
			bytes = m.Bytes()
			m.Ack()
			wg.Done()
		}
		c.Close()
		closed = true
	}

	for _, opts := range testOpts {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		// Set a large ack buffer size to make sure the background go routine
		// can flush it.
		opts = opts.SetAckBufferSize(100)
		s := server.NewServer("a", NewConsumerHandler(consumeFn, opts), server.NewOptions())
		require.NoError(t, err)
		s.Serve(l)

		conn, err := net.Dial("tcp", l.Addr().String())
		require.NoError(t, err)

		wg.Add(1)
		err = produce(conn, &testMsg1, opts)
		require.NoError(t, err)

		wg.Wait()
		require.Equal(t, testMsg1.Value, bytes)

		var ack msgpb.Ack
		testDecoder := proto.NewDecoder(conn, opts.DecoderOptions(), 10)
		err = testDecoder.Decode(&ack)
		require.NoError(t, err)
		require.Equal(t, 1, len(ack.Metadata))
		require.Equal(t, testMsg1.Metadata, ack.Metadata[0])

		s.Close()
		require.True(t, closed)
	}
}
