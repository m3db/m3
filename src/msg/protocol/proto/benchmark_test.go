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
)

func BenchmarkEncodeDecoderRoundTrip(b *testing.B) {
	r := bytes.NewReader(nil)
	c := NewEncodeDecoder(r, NewEncodeDecoderOptions()).(*encdec)
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{},
		Value:    make([]byte, 200),
	}
	decodeMsg := msgpb.Message{}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		encodeMsg.Metadata.Id = uint64(n)
		err := c.Encode(&encodeMsg)
		if err != nil {
			b.FailNow()
		}
		r.Reset(c.Bytes())
		if err := c.Decode(&decodeMsg); err != nil {
			b.FailNow()
		}
		if decodeMsg.Metadata.Id != uint64(n) {
			b.FailNow()
		}
	}
}

func BenchmarkBaseEncodeDecodeRoundTrip(b *testing.B) {
	r := bytes.NewReader(nil)
	encoder := NewEncoder(NewBaseOptions())
	decoder := NewDecoder(r, NewBaseOptions())
	encodeMsg := msgpb.Message{
		Metadata: msgpb.Metadata{},
		Value:    make([]byte, 200),
	}
	decodeMsg := msgpb.Message{}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		encodeMsg.Metadata.Id = uint64(n)
		err := encoder.Encode(&encodeMsg)
		if err != nil {
			b.FailNow()
		}
		r.Reset(encoder.Bytes())
		if err := decoder.Decode(&decodeMsg); err != nil {
			b.FailNow()
		}
		if decodeMsg.Metadata.Id != uint64(n) {
			b.FailNow()
		}
	}
}
