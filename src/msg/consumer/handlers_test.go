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
	"sort"
	"sync"
	"testing"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/server"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestServerWithSingletonMessageProcessor(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		data []string
		wg   sync.WaitGroup
		mu   sync.Mutex
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	p := NewMockMessageProcessor(ctrl)
	p.EXPECT().Process(gomock.Any()).Do(
		func(m Message) {
			mu.Lock()
			data = append(data, string(m.Bytes()))
			mu.Unlock()
			m.Ack()
			wg.Done()
		},
	).Times(3)
	// Set a large ack buffer size to make sure the background go routine
	// can flush it.
	opts := testOptions().SetAckBufferSize(100)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := server.NewServer("a", NewMessageHandler(SingletonMessageProcessor(p), opts), server.NewOptions())
	defer s.Close()
	require.NoError(t, s.Serve(l))

	conn1, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	conn2, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	wg.Add(3)
	err = produce(conn1, &testMsg1)
	require.NoError(t, err)
	err = produce(conn1, &testMsg2)
	require.NoError(t, err)
	err = produce(conn2, &testMsg2)
	require.NoError(t, err)

	wg.Wait()
	sort.Strings(data)
	require.Equal(t, string(testMsg2.Value), data[0])
	require.Equal(t, string(testMsg2.Value), data[1])
	require.Equal(t, string(testMsg1.Value), data[2])

	var ack msgpb.Ack
	testDecoder := proto.NewDecoder(conn1, opts.DecoderOptions(), 10)
	err = testDecoder.Decode(&ack)
	require.NoError(t, err)
	testDecoder = proto.NewDecoder(conn2, opts.DecoderOptions(), 10)
	err = testDecoder.Decode(&ack)
	require.NoError(t, err)
	require.Equal(t, 3, len(ack.Metadata))
	sort.Slice(ack.Metadata, func(i, j int) bool {
		return ack.Metadata[i].Id < ack.Metadata[j].Id
	})
	require.Equal(t, testMsg1.Metadata, ack.Metadata[0])
	require.Equal(t, testMsg2.Metadata, ack.Metadata[1])
	require.Equal(t, testMsg2.Metadata, ack.Metadata[2])
	p.EXPECT().Close()
}

func TestServerMessageDifferentConnections(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	handleMessage := func(m Message) {
		wg.Done()
	}

	mp1 := NewMockMessageProcessor(ctrl)
	mp2 := NewMockMessageProcessor(ctrl)
	mp1.EXPECT().Process(gomock.Any()).Do(handleMessage)
	mp1.EXPECT().Close()
	mp2.EXPECT().Process(gomock.Any()).Do(handleMessage)
	mp2.EXPECT().Close()

	// Set a large ack buffer size to make sure the background go routine
	// can flush it.
	opts := testOptions().SetAckBufferSize(100)
	first := true
	var mu sync.Mutex
	newMessageProcessor := func() MessageProcessor {
		mu.Lock()
		defer mu.Unlock()
		if first {
			first = false
			return mp1
		}
		return mp2
	}

	s := server.NewServer("a",
		NewMessageHandler(NewMessageProcessorFactory(newMessageProcessor), opts), server.NewOptions())
	require.NoError(t, err)
	require.NoError(t, s.Serve(l))

	conn1, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	conn2, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)

	err = produce(conn1, &testMsg1)
	require.NoError(t, err)
	err = produce(conn2, &testMsg1)
	require.NoError(t, err)

	wg.Wait()
	s.Close()
}
