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

package writer

import (
	"testing"

	"github.com/m3db/m3/src/msg/producer"
)

var (
	// BenchMessage prevents optimization
	BenchMessage *message
	// BenchBool prevents optimization
	BenchBool bool
)

type emptyMessage struct{}

// Shard returns the shard of the message.
func (e emptyMessage) Shard() uint32 { return 0 }

// Bytes returns the bytes of the message.
func (e emptyMessage) Bytes() []byte { return nil }

// Size returns the size of the bytes of the message.
func (e emptyMessage) Size() int { return 0 }

// Finalize will be called by producer to indicate the end of its lifecycle.
func (e emptyMessage) Finalize(_ producer.FinalizeReason) {}

func BenchmarkMessageAtomics(b *testing.B) {
	rm := producer.NewRefCountedMessage(emptyMessage{}, nil)
	msg := newMessage()
	for n := 0; n < b.N; n++ {
		msg.Set(metadata{}, rm, 500)
		rm.IncRef()
		msg.Ack()
		BenchBool = msg.IsAcked()
		_, BenchBool = msg.Marshaler()
		msg.Close()
		BenchMessage = msg
	}
}

func BenchmarkMessageAtomicsAllocs(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rm := producer.NewRefCountedMessage(emptyMessage{}, nil)
		msg := newMessage()
		msg.Set(metadata{}, rm, 500)
		rm.IncRef()
		msg.Ack()
		BenchBool = msg.IsAcked()
		_, BenchBool = msg.Marshaler()
		msg.Close()
		BenchMessage = msg
	}
}
