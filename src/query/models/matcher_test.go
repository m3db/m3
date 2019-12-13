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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatcherString(t *testing.T) {
	m, err := NewMatcher(MatchEqual, []byte("foo"), []byte("bar"))
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, `foo="bar"`, m.String())
	assert.Equal(t, `foo="bar"`, (&m).String())
}

func TestMatchType(t *testing.T) {
	require.Equal(t, MatchEqual.String(), "=")
}

func TestMatchersFromEmptyString(t *testing.T) {
	matchers, err := MatchersFromString("")
	assert.NoError(t, err)
	assert.Len(t, matchers, 0)
}

func TestMatchersFromStringErrors(t *testing.T) {
	_, err := MatchersFromString(":")
	assert.Error(t, err)
	_, err = MatchersFromString(":aa")
	assert.Error(t, err)
}

func TestValidMatchersFromString(t *testing.T) {
	m, err := MatchersFromString("a:")
	assert.NoError(t, err)
	expected := Matchers{{
		Name:  []byte("a"),
		Value: []byte{},
		Type:  MatchRegexp,
	}}

	assert.Equal(t, expected, m)

	m, err = MatchersFromString("a:aa")
	expected[0].Value = []byte("aa")
	assert.NoError(t, err)
	assert.Equal(t, expected, m)

	m, err = MatchersFromString("a:aa      b:c")
	expected = append(expected, Matcher{
		Name:  []byte("b"),
		Value: []byte("c"),
		Type:  MatchRegexp,
	})

	assert.NoError(t, err)
	assert.Equal(t, expected, m)
}
