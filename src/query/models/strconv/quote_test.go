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

package strconv

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

type quoteTest struct {
	in  string
	out string
}

var quotetests = []quoteTest{
	// NB: original strconv tests
	{"\a\b\f\r\n\t\v", `"\a\b\f\r\n\t\v"`},
	{"\\", `"\\"`},
	{"abc\xffdef", `"abc\xffdef"`},
	{"\u263a", `"☺"`},
	{"\U0010ffff", `"\U0010ffff"`},
	{"\x04", `"\x04"`},
	// Some non-printable but graphic runes. Final column is double-quoted.
	{"!\u00a0!\u2000!\u3000!", `"!\u00a0!\u2000!\u3000!"`},

	// NB: Additional tests
	{`"tag"`, `"\"tag\""`},
	{`"t"a"g"`, `"\"t\"a\"g\""`},
}

func TestEscape(t *testing.T) {
	for _, tt := range quotetests {
		in := []byte(tt.in)
		bufferLen := EscapedLength(in)
		bb := make([]byte, bufferLen)
		idx := Escape(bb, in, 0)
		assert.Equal(t, idx, bufferLen)
		expected := []byte(tt.out)
		assert.Equal(t, expected[1:len(expected)-1], bb)
	}
}

func TestQuote(t *testing.T) {
	for _, tt := range quotetests {
		in := []byte(tt.in)
		bufferLen := QuotedLength(in)
		bb := make([]byte, bufferLen)
		idx := Quote(bb, in, 0)
		assert.Equal(t, idx, bufferLen)
		assert.Equal(t, []byte(tt.out), bb)
	}
}

func TestQuoteWithOffset(t *testing.T) {
	for _, tt := range quotetests {
		in := []byte(tt.in)
		bufferLen := QuotedLength(in)
		bb := make([]byte, bufferLen+2)
		bb[0] = '!'
		bb[bufferLen+1] = '!'
		idx := Quote(bb, in, 1)
		assert.Equal(t, idx, bufferLen+1)
		assert.Equal(t, []byte("!"+tt.out+"!"), bb)
	}
}

func TestSimpleQuote(t *testing.T) {
	for _, tt := range quotetests {
		in := []byte(tt.in)
		// accounts for buffer and 2 quotation characters
		bufferLen := len(in) + 2
		bb := make([]byte, bufferLen)
		idx := QuoteSimple(bb, in, 0)
		assert.Equal(t, idx, bufferLen)
		expected := []byte("\"" + tt.in + "\"")
		assert.Equal(t, expected, bb)
	}
}

func TestSimpleQuoteWithOffset(t *testing.T) {
	for _, tt := range quotetests {
		in := []byte(tt.in)
		// accounts for buffer, additional characters, and 2 quotation characters
		bufferLen := len(in) + 4
		bb := make([]byte, bufferLen)
		bb[0] = '!'
		bb[bufferLen-1] = '!'
		idx := QuoteSimple(bb, in, 1)
		assert.Equal(t, idx, bufferLen-1)
		expected := []byte("!\"" + tt.in + "\"!")
		assert.Equal(t, expected, bb)
	}
}

func TestLongQuoteWithOffset(t *testing.T) {
	for _, tt := range quotetests {
		repeat := 100
		in := []byte(tt.in)
		bufferLen := QuotedLength(in)
		bb := make([]byte, bufferLen*repeat)
		for i := 0; i < repeat; i++ {
			idx := Quote(bb, in, bufferLen*i)
			assert.Equal(t, idx, bufferLen*(i+1))
			expected := ""
			for j := 0; j <= i; j++ {
				expected += tt.out
			}

			assert.Equal(t, []byte(expected), bb[0:idx])
		}
	}
}

func BenchmarkQuoteSimple(b *testing.B) {
	src := []byte("\a\b\f\r\n\t\v\a\b\f\r\n\t\v\a\b\f\r\n\t\v")
	dst := make([]byte, len(src)+2)
	for i := 0; i < b.N; i++ {
		QuoteSimple(dst, src, 0)
	}
}

func BenchmarkQuote(b *testing.B) {
	src := []byte("\a\b\f\r\n\t\v\a\b\f\r\n\t\v\a\b\f\r\n\t\v")
	dst := make([]byte, QuotedLength(src))
	for i := 0; i < b.N; i++ {
		Quote(dst, src, 0)
	}
}

func BenchmarkQuoteWithOffset(b *testing.B) {
	src := []byte("\a\b\f\r\n\t\v\a\b\f\r\n\t\v\a\b\f\r\n\t\v")
	l := QuotedLength(src)
	dst := make([]byte, l*100)
	for i := 0; i < b.N; i++ {
		Quote(dst, src, l*(i%100))
	}
}

// NB: original utf8.EncodeRune tests
var utf8map = []struct {
	r   rune
	str string
}{
	{0x0000, "\x00"},
	{0x0001, "\x01"},
	{0x007e, "\x7e"},
	{0x007f, "\x7f"},
	{0x0080, "\xc2\x80"},
	{0x0081, "\xc2\x81"},
	{0x00bf, "\xc2\xbf"},
	{0x00c0, "\xc3\x80"},
	{0x00c1, "\xc3\x81"},
	{0x00c8, "\xc3\x88"},
	{0x00d0, "\xc3\x90"},
	{0x00e0, "\xc3\xa0"},
	{0x00f0, "\xc3\xb0"},
	{0x00f8, "\xc3\xb8"},
	{0x00ff, "\xc3\xbf"},
	{0x0100, "\xc4\x80"},
	{0x07ff, "\xdf\xbf"},
	{0x0400, "\xd0\x80"},
	{0x0800, "\xe0\xa0\x80"},
	{0x0801, "\xe0\xa0\x81"},
	{0x1000, "\xe1\x80\x80"},
	{0xd000, "\xed\x80\x80"},
	{0xd7ff, "\xed\x9f\xbf"}, // last code point before surrogate half.
	{0xe000, "\xee\x80\x80"}, // first code point after surrogate half.
	{0xfffe, "\xef\xbf\xbe"},
	{0xffff, "\xef\xbf\xbf"},
	{0x10000, "\xf0\x90\x80\x80"},
	{0x10001, "\xf0\x90\x80\x81"},
	{0x40000, "\xf1\x80\x80\x80"},
	{0x10fffe, "\xf4\x8f\xbf\xbe"},
	{0x10ffff, "\xf4\x8f\xbf\xbf"},
	{0xFFFD, "\xef\xbf\xbd"},
}

func TestEncodeRune(t *testing.T) {
	for _, m := range utf8map {
		b := []byte(m.str)
		var bb [10]byte
		n := encodeRune(bb[:], m.r, 0)
		b1 := bb[0:n]
		if !bytes.Equal(b, b1) {
			t.Errorf("EncodeRune(%#04x) = %q want %q", m.r, b1, b)
		}
	}
}

func TestEncodeRuneWithOffset(t *testing.T) {
	for _, m := range utf8map {
		b := []byte("!" + m.str)
		var bb [10]byte
		bb[0] = '!'
		n := encodeRune(bb[:], m.r, 1)
		b1 := bb[0:n]
		if !bytes.Equal(b, b1) {
			t.Errorf("EncodeRune(%#04x) = %q want %q", m.r, b1, b)
		}
	}
}
