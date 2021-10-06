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

package serialize

import (
	"fmt"
	"testing"

	"github.com/m3db/m3/src/x/ident"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

var (
	testParams *gopter.TestParameters
)

func init() {
	testParams = gopter.DefaultTestParameters()
	testParams.MinSuccessfulTests = 10000
	testParams.MaxSize = 12
}

func TestPropertySerializationBijective(t *testing.T) {
	properties := gopter.NewProperties(testParams)
	properties.Property("serialization is bijiective", prop.ForAll(
		func(x string) (bool, error) {
			tags := ident.NewTagsIterator(ident.NewTags(ident.StringTag(x, x)))
			copy, err := encodeAndDecode(tags)
			if err != nil {
				return false, err
			}
			return tagItersAreEqual(tags, copy)
		},
		gen.AnyString().SuchThat(func(x string) bool { return len(x) > 0 }),
	))
	properties.TestingRun(t)
}

func TestPropertyAnyStringsDontCollide(t *testing.T) {
	properties := gopter.NewProperties(testParams)
	properties.Property("no collisions during string concat", prop.ForAll(
		func(tag ident.Tag) (bool, error) {
			tags := ident.NewTagsIterator(ident.NewTags(tag))
			copy, err := encodeAndDecode(tags)
			if err != nil {
				return false, err
			}
			return tagItersAreEqual(tags, copy)
		}, anyTag(),
	))

	properties.TestingRun(t)
}

func TestPropertyAnyReasonableTagSlicesAreAight(t *testing.T) {
	properties := gopter.NewProperties(testParams)
	properties.Property("tags of reasonable length are handled fine", prop.ForAll(
		func(tags ident.Tags) (bool, error) {
			iter := ident.NewTagsIterator(tags)
			copy, err := encodeAndDecode(iter)
			if err != nil {
				return false, err
			}
			return tagItersAreEqual(iter, copy)
		},
		anyTags().WithLabel("input tags"),
	))

	properties.TestingRun(t)
}

func TestPropertyTagsWithMaximumLengthLiteralsAreFine(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 5
	properties := gopter.NewProperties(params)
	properties.Property("tags with maximum length literals are handled fine", prop.ForAllNoShrink(
		func(tags ident.Tags) (bool, error) {
			iter := ident.NewTagsIterator(tags)
			newIter, err := encodeAndDecode(iter)
			if err != nil {
				return false, err
			}
			return tagItersAreEqual(iter, newIter)
		},
		maximumLiteralLengthTags().WithLabel("input tags"),
	))

	properties.TestingRun(t)
}

func encodeAndDecode(t ident.TagIterator) (ident.TagIterator, error) {
	copy := t.Duplicate()
	enc := newTagEncoder(defaultNewCheckedBytesFn, newTestEncoderOpts(), nil)
	if err := enc.Encode(copy); err != nil {
		return nil, err
	}
	data, ok := enc.Data()
	if !ok {
		return nil, fmt.Errorf("unable to retrieve data")
	}
	dec := newTagDecoder(testDecodeOpts, nil)
	dec.Reset(data)
	return dec, nil
}

func tagItersAreEqual(ti1, ti2 ident.TagIterator) (bool, error) {
	ti1Next := ti1.Next()
	ti2Next := ti2.Next()

	if ti1Next != ti2Next {
		_, err := iterErrCheck(ti1, ti2)
		return false, fmt.Errorf("un-equal next check, err: %v", err)
	}

	if !ti1Next && !ti2Next {
		return iterErrCheck(ti1, ti2)
	}

	t1, t2 := ti1.Current(), ti2.Current()
	if !t1.Name.Equal(t2.Name) {
		return false, fmt.Errorf("tag names are un-equal: %v %v",
			t1.Name.Bytes(), t2.Name.Bytes())
	}
	if !t2.Value.Equal(t2.Value) {
		return false, fmt.Errorf("tag values are un-equal: %v %v",
			t1.Value.Bytes(), t2.Value.Bytes())
	}

	return tagItersAreEqual(ti1, ti2)
}

func iterErrCheck(ti1, ti2 ident.TagIterator) (bool, error) {
	err1 := ti1.Err()
	err2 := ti2.Err()
	if err1 == nil && err2 == nil {
		return true, nil
	}
	if err2 == err1 {
		return true, nil
	}
	return false, fmt.Errorf("%v %v", err1, err2)
}

func anyTag() gopter.Gen {
	limit := int(testDecodeOpts.TagSerializationLimits().MaxTagLiteralLength())
	return gopter.CombineGens(gen.Identifier(), gen.Identifier()).
		SuchThat(func(values []interface{}) bool {
			name := values[0].(string)
			if len(name) > limit && len(name) > 0 {
				return false
			}
			value := values[1].(string)
			return len(value) <= limit && len(value) > 0
		}).
		Map(func(values []interface{}) ident.Tag {
			name := values[0].(string)
			value := values[1].(string)
			return ident.StringTag(name, value)
		})
}

func anyTags() gopter.Gen {
	return gen.SliceOf(anyTag()).
		Map(func(tags []ident.Tag) ident.Tags {
			return ident.NewTags(tags...)
		})
}

func maximumLiteralLengthTag() gopter.Gen {
	limit := int(testDecodeOpts.TagSerializationLimits().MaxTagLiteralLength())
	return gopter.CombineGens(gen.SliceOfN(limit, gen.AlphaNumChar()), gen.SliceOfN(limit, gen.AlphaNumChar())).
		Map(func(values []interface{}) ident.Tag {
			name := string(values[0].([]rune))
			value := string(values[1].([]rune))
			return ident.StringTag(name, value)
		})
}

func maximumLiteralLengthTags() gopter.Gen {
	return gen.SliceOfN(1, maximumLiteralLengthTag()).
		Map(func(tags []ident.Tag) ident.Tags {
			return ident.NewTags(tags...)
		})
}
