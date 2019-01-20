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

package unsafe

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithStringSmallString(t *testing.T) {
	str := []byte("foobarbaz")
	validateWithString(t, str)
}

func TestWithStringLargeString(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 65536; i++ {
		buf.WriteByte(byte(i % 256))
	}
	str := buf.Bytes()
	validateWithString(t, str)
}

func TestWithStringAndArgSmallString(t *testing.T) {
	str := []byte("foobarbaz")
	validateWithStringAndArg(t, str)
}

func TestWithStringAndArgLargeString(t *testing.T) {
	var buf bytes.Buffer
	for i := 0; i < 65536; i++ {
		buf.WriteByte(byte(i % 256))
	}
	str := buf.Bytes()
	validateWithStringAndArg(t, str)
}

var withStringBenchSink string

func BenchmarkWithString(b *testing.B) {
	str := []byte("foobarbaz")
	WithString(str, func(s string) {
		withStringBenchSink = s
	})
}

func validateWithString(t *testing.T, b []byte) {
	WithString(b, func(str string) {
		require.Equal(t, []byte(str), []byte(b))
		require.Equal(t, len(str), len(b))
	})
}

func validateWithStringAndArg(t *testing.T, b []byte) {
	WithStringAndArg(b, "cat", func(str string, arg interface{}) {
		var buf bytes.Buffer
		buf.WriteString(str)
		buf.WriteString(arg.(string))
		require.Equal(t, string(b)+"cat", buf.String())
	})
}
