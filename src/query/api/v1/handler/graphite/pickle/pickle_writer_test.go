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

package pickle

import (
	"bytes"
	"math"
	"testing"

	"github.com/hydrogen18/stalecucumber"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteEmptyDict(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.BeginDict()
	w.EndDict()
	require.NoError(t, w.Close())

	var m map[interface{}]interface{}
	require.NoError(t, unpickle(buf.Bytes(), &m))
	assert.Equal(t, map[interface{}]interface{}{}, m)
}

func TestWriteEmptyList(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.BeginList()
	w.EndList()
	require.NoError(t, w.Close())

	var m []string
	require.NoError(t, unpickle(buf.Bytes(), &m))
	assert.Equal(t, []string{}, m)
}

func TestWriteComplex(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.BeginDict()
	w.WriteDictKey("step")
	w.WriteInt(3494945)
	w.WriteDictKey("pi")
	w.WriteFloat64(3.45e10)
	w.WriteDictKey("none")
	w.WriteNone()
	w.WriteDictKey("noNumber")
	w.WriteFloat64(math.NaN())
	w.WriteDictKey("skey")
	w.WriteString("hello world")
	w.WriteDictKey("nested")
	w.BeginDict()
	w.WriteDictKey("fooBar")
	w.BeginList()
	w.WriteFloat64(349439.3494)
	w.WriteInt(-9459450)
	w.WriteString("A Nested String")
	w.EndList()
	w.EndDict()
	w.EndDict()
	require.NoError(t, w.Close())

	s := struct {
		Step     int
		Pi       float64
		NoNumber *float64
		Skey     string
		Nested   struct {
			FooBar []interface{}
		}
	}{}

	require.NoError(t, unpickle(buf.Bytes(), &s))
	assert.Equal(t, 3494945, s.Step)
	assert.Equal(t, 3.45e10, s.Pi)
	assert.Nil(t, s.NoNumber)
	assert.Equal(t, "hello world", s.Skey)
	assert.Equal(t, []interface{}{
		349439.3494, int64(-9459450), "A Nested String",
	}, s.Nested.FooBar)
}

func unpickle(b []byte, data interface{}) error {
	r := bytes.NewReader(b)
	return stalecucumber.UnpackInto(data).From(stalecucumber.Unpickle(r))
}
