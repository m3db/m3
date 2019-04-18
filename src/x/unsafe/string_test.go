// Copyright (c) 2016 Uber Technologies, Inc.
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

package unsafe

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithBytesSmallString(t *testing.T) {
	str := "foobarbaz"
	validateWithBytes(t, str)
}

func TestWithBytesLargeString(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 65536; i++ {
		buf.WriteByte(byte(i % 256))
	}
	str := buf.String()
	validateWithBytes(t, str)
}

func TestWithBytesAndArgSmallString(t *testing.T) {
	str := "foobarbaz"
	validateWithBytesAndArg(t, str)
}

func TestWithBytesAndArgLargeString(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 65536; i++ {
		buf.WriteByte(byte(i % 256))
	}
	str := buf.String()
	validateWithBytesAndArg(t, str)
}

var withBytesBenchSink ImmutableBytes

func BenchmarkWithBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		WithBytes("foobar", func(b ImmutableBytes) {
			withBytesBenchSink = b
		})
	}
}

func validateWithBytes(t *testing.T, str string) {
	WithBytes(str, func(b ImmutableBytes) {
		require.Equal(t, []byte(str), []byte(b))
		require.Equal(t, len(str), len(b))
		require.Equal(t, len(str), cap(b))
	})
}

func validateWithBytesAndArg(t *testing.T, str string) {
	WithBytesAndArg(str, "cat", func(data ImmutableBytes, arg interface{}) {
		var buf bytes.Buffer
		for _, b := range data {
			buf.WriteByte(b)
		}
		buf.WriteString(arg.(string))
		require.Equal(t, str+"cat", buf.String())
	})
}
