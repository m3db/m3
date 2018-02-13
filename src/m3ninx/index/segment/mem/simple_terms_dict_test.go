// Copyright (c) 2017 Uber Technologies, Inc.
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

package mem

import (
	"testing"

	"github.com/m3db/m3ninx/doc"

	"github.com/stretchr/testify/suite"
)

type newSimpleTermsDictFn func() *simpleTermsDictionary

type simpleTermsDictionaryTestSuite struct {
	suite.Suite

	fn        newSimpleTermsDictFn
	termsDict *simpleTermsDictionary
}

func (t *simpleTermsDictionaryTestSuite) SetupTest() {
	t.termsDict = t.fn()
}

func (t *simpleTermsDictionaryTestSuite) TestInsert() {
	err := t.termsDict.Insert(doc.Field{
		Name:  []byte("abc"),
		Value: doc.Value("efg"),
	}, 1)
	t.NoError(err)

	ids, err := t.termsDict.Fetch([]byte("abc"), []byte("efg"), termFetchOptions{false})
	t.NoError(err)
	t.NotNil(ids)
	t.Equal(uint64(1), ids.Size())
	t.True(ids.Contains(1))
}

func (t *simpleTermsDictionaryTestSuite) TestInsertIdempotent() {
	err := t.termsDict.Insert(doc.Field{
		Name:  []byte("abc"),
		Value: doc.Value("efg"),
	}, 1)
	t.NoError(err)
	err = t.termsDict.Insert(doc.Field{
		Name:  []byte("abc"),
		Value: doc.Value("efg"),
	}, 1)
	t.NoError(err)

	ids, err := t.termsDict.Fetch([]byte("abc"), []byte("efg"), termFetchOptions{false})
	t.NoError(err)
	t.NotNil(ids)
	t.Equal(uint64(1), ids.Size())
	t.True(ids.Contains(1))
}

func (t *simpleTermsDictionaryTestSuite) TestFetchRegex() {
	err := t.termsDict.Insert(doc.Field{
		Name:  []byte("abc"),
		Value: doc.Value("efg"),
	}, 1)
	t.NoError(err)
	err = t.termsDict.Insert(doc.Field{
		Name:  []byte("abc"),
		Value: doc.Value("efgh"),
	}, 2)
	t.NoError(err)

	ids, err := t.termsDict.Fetch([]byte("abc"), []byte("efg.*"), termFetchOptions{true})
	t.NoError(err)
	t.NotNil(ids)
	t.Equal(uint64(2), ids.Size())
	t.True(ids.Contains(1))
	t.True(ids.Contains(2))
}

func TestSimpleTermsDictionary(t *testing.T) {
	opts := NewOptions()
	suite.Run(t, &simpleTermsDictionaryTestSuite{
		fn: func() *simpleTermsDictionary {
			return newSimpleTermsDictionary(opts)
		},
	})
}
