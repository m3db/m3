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
	"fmt"
	"testing"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"
)

func BenchmarkEncoder(b *testing.B) {
	b.Run("with non custom encoded fields enabled", func(b *testing.B) {
		benchmarkEncoder(b, true)
	})
	b.Run("with non custom encoded fields disabled", func(b *testing.B) {
		benchmarkEncoder(b, false)
	})
}

func benchmarkEncoder(b *testing.B, nonCustomFieldsEnabled bool) {
	var (
		_, messagesBytes = testMessages(100, nonCustomFieldsEnabled)
		start            = xtime.Now()
		encoder          = NewEncoder(start, encoding.NewOptions())
	)
	encoder.SetSchema(namespace.GetTestSchemaDescr(testVLSchema))

	for i := 0; i < b.N; i++ {
		start = start.Add(time.Second)
		for _, protoBytes := range messagesBytes {
			if err := encoder.Encode(ts.Datapoint{TimestampNanos: start},
				xtime.Second, protoBytes); err != nil {
				panic(err)
			}
		}
	}
}

func BenchmarkIterator(b *testing.B) {
	b.Run("with non custom encoded fields enabled", func(b *testing.B) {
		benchmarkIterator(b, true)
	})
	b.Run("with non custom encoded fields disabled", func(b *testing.B) {
		benchmarkIterator(b, false)
	})
}

func benchmarkIterator(b *testing.B, nonCustomFieldsEnabled bool) {
	ctx := context.NewBackground()
	defer ctx.Close()

	var (
		_, messagesBytes = testMessages(100, nonCustomFieldsEnabled)
		start            = xtime.Now()
		encodingOpts     = encoding.NewOptions()
		encoder          = NewEncoder(start, encodingOpts)
		schema           = namespace.GetTestSchemaDescr(testVLSchema)
	)
	encoder.SetSchema(schema)

	for _, protoBytes := range messagesBytes {
		start = start.Add(time.Second)
		if err := encoder.Encode(ts.Datapoint{TimestampNanos: start},
			xtime.Second, protoBytes); err != nil {
			panic(err)
		}
	}

	stream, ok := encoder.Stream(ctx)
	if !ok {
		panic("encoder had no stream")
	}
	segment, err := stream.Segment()
	handleErr(err)

	iterator := NewIterator(stream, schema, encodingOpts)
	reader := xio.NewSegmentReader(segment)
	for i := 0; i < b.N; i++ {
		reader.Reset(segment)
		iterator.Reset(reader, schema)
		for iterator.Next() {
			iterator.Current()
		}
		handleErr(iterator.Err())
	}
}

func testMessages(numMessages int, includeAttributes bool) ([]*dynamic.Message, [][]byte) {
	var (
		messages      = make([]*dynamic.Message, 0, numMessages)
		messagesBytes = make([][]byte, 0, numMessages)
	)
	for i := 0; i < numMessages; i++ {
		m := dynamic.NewMessage(testVLSchema)
		m.SetFieldByName("latitude", float64(i))
		m.SetFieldByName("longitude", float64(i))
		m.SetFieldByName("deliveryID", []byte(fmt.Sprintf("some-really-really-really-really-long-id-%d", i)))
		if includeAttributes {
			m.SetFieldByName("attributes", map[string]string{
				fmt.Sprintf("key1_%d", i): fmt.Sprintf("val1_%d", i),
				fmt.Sprintf("key2_%d", i): fmt.Sprintf("val2_%d", i),
				fmt.Sprintf("key3_%d", i): fmt.Sprintf("val3_%d", i),
			})
		}

		bytes, err := m.Marshal()
		handleErr(err)

		messagesBytes = append(messagesBytes, bytes)
		messages = append(messages, m)
	}
	return messages, messagesBytes
}

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
