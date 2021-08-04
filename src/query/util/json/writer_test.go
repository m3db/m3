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

package json

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteValues(t *testing.T) {
	testWrite(t, "true", func(w Writer) { w.WriteBool(true) })
	testWrite(t, "false", func(w Writer) { w.WriteBool(false) })
	testWrite(t, "3.145000", func(w Writer) { w.WriteFloat64(3.145) })
	testWrite(t, "null", func(w Writer) { w.WriteFloat64(math.NaN()) })
	testWrite(t, "null", func(w Writer) { w.WriteFloat64(math.Inf(1)) })
	testWrite(t, "null", func(w Writer) { w.WriteFloat64(math.Inf(-1)) })
	testWrite(t, "26756", func(w Writer) { w.WriteInt(26756) })
	testWrite(t, "null", func(w Writer) { w.WriteNull() })
	testWrite(t, "\"Hello\\t \\r \\\" World\"", func(w Writer) {
		w.WriteString("Hello\t \r \" World")
	})
	testWrite(t, "\"Hello\\t \\r \\\" World\"", func(w Writer) {
		w.WriteBytesString([]byte("Hello\t \r \" World"))
	})
	maxTestUTF8Value := 1032
	type utf8FnTest func(w Writer, s string)
	for _, fn := range []utf8FnTest{
		utf8FnTest(func(w Writer, s string) { w.WriteString(s) }),
		utf8FnTest(func(w Writer, s string) { w.WriteBytesString([]byte(s)) }),
	} {
		fn := fn // Capture for lambdas.
		for i := 0; i <= maxTestUTF8Value; i++ {
			i := i
			switch {
			case i == int('"') || i == int('\\'):
				testWrite(t, fmt.Sprintf("\"\\%c\"", rune(i)), func(w Writer) { fn(w, fmt.Sprintf("%c", rune(i))) })
			case i == int('\n'):
				testWrite(t, "\"\\n\"", func(w Writer) { fn(w, fmt.Sprintf("%c", rune(i))) })
			case i == int('\r'):
				testWrite(t, "\"\\r\"", func(w Writer) { fn(w, fmt.Sprintf("%c", rune(i))) })
			case i == int('\t'):
				testWrite(t, "\"\\t\"", func(w Writer) { fn(w, fmt.Sprintf("%c", rune(i))) })
			case i <= 31:
				testWrite(t,
					fmt.Sprintf("\"\\u%s\"", fmt.Sprintf("%U", i)[2:]),
					func(w Writer) { fn(w, fmt.Sprintf("%c", rune(i))) })
			default:
				testWrite(t, fmt.Sprintf("\"%c\"", rune(i)), func(w Writer) { fn(w, fmt.Sprintf("%c", rune(i))) })
			}
		}
	}
}

func TestWriteObject(t *testing.T) {
	testWrite(t,
		"{\"foo\":null,\"bar\":3.145000,\"zed\":\"Hello World\",\"nan\":null,\"infinity\":null,\"bad\\u0006\":null}",
		func(w Writer) {
			w.BeginObject()
			w.BeginObjectField("foo")
			w.WriteNull()
			w.BeginObjectBytesField([]byte("bar"))
			w.WriteFloat64(3.145)
			w.BeginObjectField("zed")
			w.WriteString("Hello World")
			w.BeginObjectField("nan")
			w.WriteFloat64(math.NaN())
			w.BeginObjectField("infinity")
			w.WriteFloat64(math.Inf(-1))
			w.BeginObjectField("bad\x06")
			w.WriteNull()
			w.EndObject()
		})
}

func TestWriteArray(t *testing.T) {
	testWrite(t, "[\"Hello World\",3.145000,null,24,false,null,null]", func(w Writer) {
		w.BeginArray()
		w.WriteString("Hello World")
		w.WriteFloat64(3.145)
		w.WriteNull()
		w.WriteInt(24)
		w.WriteBool(false)
		w.WriteFloat64(math.NaN())
		w.WriteFloat64(math.Inf(1))
		w.EndArray()
	})
}

func TestWriteComplexObject(t *testing.T) {
	testWrite(t, "{\"foo\":{\"bar\":{\"elements\":[\"Hello World\",3.145000,null,24,false],\"empty\":null}}}",
		func(w Writer) {
			w.BeginObject()
			w.BeginObjectField("foo")
			w.BeginObject()
			w.BeginObjectField("bar")
			w.BeginObject()
			w.BeginObjectField("elements")
			w.BeginArray()
			w.WriteString("Hello World")
			w.WriteFloat64(3.145)
			w.WriteNull()
			w.WriteInt(24)
			w.WriteBool(false)
			w.EndArray()
			w.BeginObjectField("empty")
			w.WriteNull()
			w.EndObject()
			w.EndObject()
			w.EndObject()
		})
}

func TestWriteErrors(t *testing.T) {
	testWriteError(t, "value not allowed", func(w Writer) {
		w.BeginObject()
		w.BeginArray()
	})
	testWriteError(t, "container mismatch", func(w Writer) {
		w.BeginObject()
		w.EndArray()
	})
	testWriteError(t, "container still open", func(w Writer) {
		w.BeginObject()
	})
	testWriteError(t, "not in container", func(w Writer) {
		w.EndObject()
	})
	testWriteError(t, "field not allowed", func(w Writer) {
		w.BeginObjectField("foo")
	})
	testWriteError(t, "field not allowed", func(w Writer) {
		w.BeginObject()
		w.BeginObjectField("foo")
		w.BeginObjectField("bar")
	})
	testWriteError(t, "field not allowed", func(w Writer) {
		w.BeginArray()
		w.BeginObjectField("foo")
	})
	testWriteError(t, "value not allowed", func(w Writer) {
		w.WriteBool(true)
		w.BeginObject()
	})
	testWriteError(t, "value not allowed", func(w Writer) {
		w.BeginObject()
		w.WriteFloat64(100)
	})
}

func testWriteError(t *testing.T, expected string, f func(w Writer)) {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	f(w)

	err := w.Close()
	require.Error(t, err)
	assert.Equal(t, expected, err.Error())
}

func testWrite(t *testing.T, expected string, f func(w Writer)) {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	f(w)
	require.NoError(t, w.Close(), "error writing %s", expected)
	assert.Equal(t, expected, buf.String())
}
