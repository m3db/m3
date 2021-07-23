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
	"bufio"
	"bytes"
	"net"
	"testing"

	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

func TestBaseEncodeDecodeRoundTripWithoutPool(t *testing.T) {
	enc := NewEncoder(NewOptions()).(*encoder)
	require.Equal(t, 4, len(enc.buffer))
	require.Equal(t, 4, cap(enc.buffer))
	require.Empty(t, enc.Bytes())

	r := bytes.NewReader(nil)
	buf := bufio.NewReader(r)
	dec := NewDecoder(buf, NewOptions(), 10).(*decoder)
	require.Equal(t, 4, len(dec.buffer))
	require.Equal(t, 4, cap(dec.buffer))
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 80),
	}
	decodeMsg := msgpb.Message{}

	err := enc.Encode(&encodeMsg)
	require.NoError(t, err)
	require.Equal(t, sizeEncodingLength+encodeMsg.Size(), len(enc.buffer))
	require.Equal(t, sizeEncodingLength+encodeMsg.Size(), cap(enc.buffer))

	r.Reset(enc.Bytes())
	require.NoError(t, dec.Decode(&decodeMsg))
	require.Equal(t, sizeEncodingLength+decodeMsg.Size(), len(dec.buffer))
	require.Equal(t, sizeEncodingLength+encodeMsg.Size(), cap(dec.buffer))
}

func TestBaseEncodeDecodeRoundTripWithPool(t *testing.T) {
	p := getBytesPool(2, []int{2, 8, 100})
	p.Init()

	enc := NewEncoder(NewOptions().SetBytesPool(p)).(*encoder)
	require.Equal(t, 8, len(enc.buffer))
	require.Equal(t, 8, cap(enc.buffer))

	r := bytes.NewReader(nil)
	buf := bufio.NewReader(r)
	dec := NewDecoder(buf, NewOptions().SetBytesPool(p), 10).(*decoder)
	require.Equal(t, 8, len(dec.buffer))
	require.Equal(t, 8, cap(dec.buffer))
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 80),
	}
	decodeMsg := msgpb.Message{}

	err := enc.Encode(&encodeMsg)
	require.NoError(t, err)
	require.Equal(t, 100, len(enc.buffer))
	require.Equal(t, 100, cap(enc.buffer))

	r.Reset(enc.Bytes())
	require.NoError(t, dec.Decode(&decodeMsg))
	require.Equal(t, 100, len(dec.buffer))
	require.Equal(t, 100, cap(dec.buffer))
}

func TestResetReader(t *testing.T) {
	enc := NewEncoder(nil)
	r := bytes.NewReader(nil)
	dec := NewDecoder(r, nil, 10)
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 200),
	}
	decodeMsg := msgpb.Message{}

	err := enc.Encode(&encodeMsg)
	require.NoError(t, err)
	require.Error(t, dec.Decode(&decodeMsg))

	r2 := bytes.NewReader(enc.Bytes())
	dec.(*decoder).ResetReader(r2)
	require.NoError(t, dec.Decode(&decodeMsg))
}

func TestEncodeMessageLargerThanMaxSize(t *testing.T) {
	opts := NewOptions().SetMaxMessageSize(4)
	enc := NewEncoder(opts)
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 10),
	}

	err := enc.Encode(&encodeMsg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "larger than maximum supported size")
}

func TestDecodeMessageLargerThanMaxSize(t *testing.T) {
	enc := NewEncoder(nil)
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 10),
	}

	err := enc.Encode(&encodeMsg)
	require.NoError(t, err)

	decodeMsg := msgpb.Message{}
	opts := NewOptions().SetMaxMessageSize(8)
	buf := bufio.NewReader(bytes.NewReader(enc.Bytes()))
	dec := NewDecoder(buf, opts, 10)

	// NB(r): We need to make sure does not grow the buffer
	// if over max size, so going to take size of buffer, make
	// sure its sizeEncodingLength so we can measure if it increases at all.
	require.Equal(t, sizeEncodingLength, cap(dec.(*decoder).buffer))

	err = dec.Decode(&decodeMsg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "larger than maximum supported size")

	// Make sure did not grow buffer before returning error.
	require.Equal(t, sizeEncodingLength, cap(dec.(*decoder).buffer))
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	r := bytes.NewReader(nil)
	buf := bufio.NewReader(r)

	enc := NewEncoder(nil)
	dec := NewDecoder(buf, nil, 10)

	clientConn, serverConn := net.Pipe()
	dec.ResetReader(serverConn)

	testMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 10),
	}
	go func() {
		require.NoError(t, enc.Encode(&testMsg))
		_, err := clientConn.Write(enc.Bytes())
		require.NoError(t, err)
	}()
	var msg msgpb.Message
	require.NoError(t, dec.Decode(&msg))
	require.Equal(t, testMsg, msg)
}

// nolint: unparam
func getBytesPool(bucketSizes int, bucketCaps []int) pool.BytesPool {
	buckets := make([]pool.Bucket, len(bucketCaps))
	for i, cap := range bucketCaps {
		buckets[i] = pool.Bucket{
			Count:    pool.Size(bucketSizes),
			Capacity: cap,
		}
	}

	return pool.NewBytesPool(buckets, nil)
}
