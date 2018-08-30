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

package ident

import (
	"testing"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/suite"
)

func TestSimpleIDPool(t *testing.T) {
	s := &idPoolTestSuite{
		pool: newTestSimplePool(),
	}
	suite.Run(t, s)
}

type idPoolTestSuite struct {
	suite.Suite
	pool Pool
}

func (s idPoolTestSuite) TestPoolGetClone() {
	ctx := context.NewContext()

	a := s.pool.GetStringID(ctx, "abc")
	b := s.pool.Clone(a)

	s.Require().True(a.Equal(b))

	ctx.BlockingClose()

	s.Require().Nil(a.Bytes())
	s.Require().NotEmpty(b.Bytes())
}

func (s idPoolTestSuite) TestPoolStringRefs() {
	a := s.pool.StringID("abc")
	s.Require().Equal(1, a.(*id).data.NumRef())
}

func (s idPoolTestSuite) TestPoolBinaryRefs() {
	v := checked.NewBytes([]byte("abc"), nil)
	s.Require().Equal(0, v.NumRef())

	a := s.pool.BinaryID(v)
	s.Require().Equal(1, a.(*id).data.NumRef())

	b := s.pool.Clone(a)
	s.Require().Equal(1, b.(*id).data.NumRef())
	s.Require().Equal(1, a.(*id).data.NumRef())
}

func (s idPoolTestSuite) TestPoolGetBinaryID() {
	v := checked.NewBytes([]byte("abc"), nil)
	nr := v.NumRef()

	ctx := context.NewContext()
	bid := s.pool.GetBinaryID(ctx, v)

	s.Require().Equal(1+nr, v.NumRef())

	ctx.BlockingClose()
	s.Require().Nil(bid.(*id).data)
}

func (s idPoolTestSuite) TestPoolBinaryID() {
	v := checked.NewBytes([]byte("abc"), nil)
	v.IncRef()
	nr := v.NumRef()

	bid := s.pool.BinaryID(v)
	s.Require().Equal(1+nr, v.NumRef())
	bid.Finalize()
	s.Require().Nil(bid.(*id).data)
	s.Require().NotNil(v.Bytes())
	s.Require().Equal(nr, v.NumRef())
}

func (s idPoolTestSuite) TestPoolGetBinaryTag() {
	tagName := checked.NewBytes([]byte("abc"), nil)
	tagValue := checked.NewBytes([]byte("def"), nil)
	nr := tagName.NumRef()

	ctx := context.NewContext()
	tag := s.pool.GetBinaryTag(ctx, tagName, tagValue)
	s.Require().Equal(1+nr, tagName.NumRef())
	s.Require().Equal(1+nr, tagValue.NumRef())
	ctx.BlockingClose()
	s.Require().Nil(tag.Name.(*id).data)
	s.Require().Nil(tag.Value.(*id).data)
	s.Require().Equal(nr, tagName.NumRef())
	s.Require().Equal(nr, tagValue.NumRef())
}

func (s idPoolTestSuite) TestPoolBinaryTag() {
	tagName := checked.NewBytes([]byte("abc"), nil)
	tagName.IncRef()
	tagValue := checked.NewBytes([]byte("def"), nil)
	tagValue.IncRef()
	nr := tagName.NumRef()
	vr := tagValue.NumRef()

	tag := s.pool.BinaryTag(tagName, tagValue)
	s.Require().Equal(1+nr, tagName.NumRef())
	s.Require().Equal(1+vr, tagValue.NumRef())

	tag.Finalize()
	s.Require().Nil(tag.Name)
	s.Require().NotNil(tagName.Bytes())
	s.Require().Equal(nr, tagName.NumRef())

	s.Require().Nil(tag.Value)
	s.Require().NotNil(tagValue.Bytes())
	s.Require().Equal(vr, tagValue.NumRef())
}

func (s idPoolTestSuite) TestPoolGetStringID() {
	ctx := context.NewContext()
	sid := s.pool.GetStringID(ctx, "abc")
	s.Require().Equal("abc", sid.String())

	ctx.BlockingClose()
	s.Require().Nil(sid.(*id).data)
}

func (s idPoolTestSuite) TestPoolStringID() {
	sid := s.pool.StringID("abc")
	s.Require().Equal("abc", sid.String())

	sid.Finalize()
	s.Require().Nil(sid.(*id).data)
}

func (s idPoolTestSuite) TestPoolTags() {
	tags := s.pool.Tags()
	tags.Append(s.pool.StringTag("foo", "000"))
	tags.Append(s.pool.StringTag("bar", "111"))
	s.Require().True(tags.Equal(NewTags(
		StringTag("foo", "000"),
		StringTag("bar", "111"),
	)))
	tags.Finalize()
	s.Require().Nil(tags.Values())
}

func (s idPoolTestSuite) TestPoolGetTagsIterator() {
	tags := s.pool.Tags()
	tags.Append(s.pool.StringTag("foo", "000"))
	tags.Append(s.pool.StringTag("bar", "111"))

	ctx := context.NewContext()
	iter := s.pool.GetTagsIterator(ctx)
	iter.Reset(tags)

	s.Require().True(NewTagIterMatcher(iter).Matches(
		NewTagsIterator(NewTags(
			StringTag("foo", "000"),
			StringTag("bar", "111"),
		)),
	))

	ctx.BlockingClose()

	s.Require().Nil(iter.(*tagSliceIter).backingSlice)
	s.Require().Equal(-1, iter.(*tagSliceIter).currentIdx)
}

func (s idPoolTestSuite) TestPoolTagsIterator() {
	tags := s.pool.Tags()
	tags.Append(s.pool.StringTag("foo", "000"))
	tags.Append(s.pool.StringTag("bar", "111"))

	iter := s.pool.TagsIterator()
	iter.Reset(tags)

	s.Require().True(NewTagIterMatcher(iter).Matches(
		NewTagsIterator(NewTags(
			StringTag("foo", "000"),
			StringTag("bar", "111"),
		)),
	))

	iter.Close()

	s.Require().Nil(iter.(*tagSliceIter).backingSlice)
	s.Require().Equal(-1, iter.(*tagSliceIter).currentIdx)
}

func newTestSimplePool() Pool {
	bytesPool := pool.NewCheckedBytesPool(nil, nil,
		func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, nil)
		})
	bytesPool.Init()
	return NewPool(bytesPool, PoolOptions{})
}
