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
	"bytes"
	"testing"

	"github.com/m3db/m3msg/generated/proto/msgpb"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestBaseEncodeDecodeRoundTripWithoutPool(t *testing.T) {
	mimicTCP := bytes.NewBuffer(nil)
	enc := newEncoder(mimicTCP, NewBaseOptions().SetBytesPool(nil))
	require.Equal(t, 4, len(enc.sizeBuffer))
	require.Equal(t, 4, cap(enc.sizeBuffer))
	dec := newDecoder(mimicTCP, NewBaseOptions().SetBytesPool(nil))
	require.Equal(t, 4, len(dec.sizeBuffer))
	require.Equal(t, 4, cap(dec.sizeBuffer))
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 80),
	}
	decodeMsg := msgpb.Message{}

	require.NoError(t, enc.Encode(&encodeMsg))
	require.Equal(t, encodeMsg.Size(), len(enc.dataBuffer))
	require.Equal(t, encodeMsg.Size(), cap(enc.dataBuffer))
	require.NoError(t, dec.Decode(&decodeMsg))
	require.Equal(t, decodeMsg.Size(), len(dec.dataBuffer))
	require.Equal(t, encodeMsg.Size(), cap(dec.dataBuffer))
}

func TestBaseEncodeDecodeRoundTripWithPool(t *testing.T) {
	p := getBytesPool(2, []int{2, 8, 100})
	p.Init()
	mimicTCP := bytes.NewBuffer(nil)
	enc := newEncoder(mimicTCP, NewBaseOptions().SetBytesPool(p))
	require.Equal(t, 8, len(enc.sizeBuffer))
	require.Equal(t, 8, cap(enc.sizeBuffer))
	dec := newDecoder(mimicTCP, NewBaseOptions().SetBytesPool(p))
	require.Equal(t, 8, len(dec.sizeBuffer))
	require.Equal(t, 8, cap(dec.sizeBuffer))
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 80),
	}
	decodeMsg := msgpb.Message{}

	require.NoError(t, enc.Encode(&encodeMsg))
	require.Equal(t, 100, len(enc.dataBuffer))
	require.Equal(t, 100, cap(enc.dataBuffer))
	require.NoError(t, dec.Decode(&decodeMsg))
	require.Equal(t, 100, len(dec.dataBuffer))
	require.Equal(t, 100, cap(dec.dataBuffer))
}

func TestResetReader(t *testing.T) {
	mimicTCP1 := bytes.NewBuffer(nil)
	mimicTCP2 := bytes.NewBuffer(nil)
	enc := NewEncoder(mimicTCP1, nil)
	dec := NewDecoder(mimicTCP2, nil)
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 200),
	}
	decodeMsg := msgpb.Message{}

	require.NoError(t, enc.Encode(&encodeMsg))
	require.Error(t, dec.Decode(&decodeMsg))
	dec.(*decoder).resetReader(mimicTCP1)
	require.NoError(t, dec.Decode(&decodeMsg))
}

func TestResetWriter(t *testing.T) {
	mimicTCP1 := bytes.NewBuffer(nil)
	mimicTCP2 := bytes.NewBuffer(nil)
	enc := NewEncoder(mimicTCP1, nil)
	dec := NewDecoder(mimicTCP2, nil)
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{
			Shard: 1,
			Id:    2,
		},
		Value: make([]byte, 200),
	}
	decodeMsg := msgpb.Message{}

	require.NoError(t, enc.Encode(&encodeMsg))
	require.Error(t, dec.Decode(&decodeMsg))
	enc.(*encoder).resetWriter(mimicTCP2)
	require.NoError(t, enc.Encode(&encodeMsg))
	require.NoError(t, dec.Decode(&decodeMsg))
}

// nolint: unparam
func getBytesPool(bucketSizes int, bucketCaps []int) pool.BytesPool {
	buckets := make([]pool.Bucket, len(bucketCaps))
	for i, cap := range bucketCaps {
		buckets[i] = pool.Bucket{
			Count:    bucketSizes,
			Capacity: cap,
		}
	}

	return pool.NewBytesPool(buckets, nil)
}
