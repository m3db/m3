// Copyright (c) 2019 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

func BenchmarkEncode(b *testing.B) {
	m1 := dynamic.NewMessage(testVLSchema)
	m1.SetFieldByName("latitude", 1.0)
	m1.SetFieldByName("longitude", 1.0)
	m1.SetFieldByName("deliveryID", []byte("some-really-really-really-really-long-id-1"))
	m1Bytes, err := m1.Marshal()
	handleErr(err)

	m2 := dynamic.NewMessage(testVLSchema)
	m2.SetFieldByName("latitude", 2.0)
	m2.SetFieldByName("longitude", 2.0)
	m2.SetFieldByName("deliveryID", []byte("some-really-really-really-really-long-id-2"))
	m2Bytes, err := m2.Marshal()
	handleErr(err)

	m3 := dynamic.NewMessage(testVLSchema)
	m3.SetFieldByName("latitude", 3.0)
	m3.SetFieldByName("longitude", 3.0)
	m3.SetFieldByName("deliveryID", []byte("some-really-really-really-really-long-id-3"))
	m3Bytes, err := m3.Marshal()
	handleErr(err)

	m4 := dynamic.NewMessage(testVLSchema)
	m4.SetFieldByName("latitude", 4.0)
	m4.SetFieldByName("longitude", 4.0)
	m4.SetFieldByName("deliveryID", []byte("some-really-really-really-really-long-id-4"))
	m4Bytes, err := m4.Marshal()
	handleErr(err)

	messages := [][]byte{
		m1Bytes, m2Bytes, m3Bytes, m4Bytes,
	}
	start := time.Now()
	encoder := NewEncoder(start, encoding.NewOptions())
	encoder.SetSchema(testVLSchema)

	for i := 0; i < b.N; i++ {
		start = start.Add(time.Second)
		protoBytes := messages[i%len(messages)]
		if err := encoder.Encode(ts.Datapoint{Timestamp: start}, xtime.Second, protoBytes); err != nil {
			panic(err)
		}
	}
}

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
