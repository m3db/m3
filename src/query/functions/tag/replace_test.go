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

package tag

import (
	"regexp"
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplace(t *testing.T) {
	tags := test.StringTagsToTags(test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}})
	regex, err := regexp.Compile("f(.*)o")
	require.NoError(t, err)
	tag, valid := addTagIfFoundAndValid(tags, []byte("foo"),
		[]byte("new"), []byte("a$1-"), regex)

	assert.True(t, valid)
	assert.Equal(t, []byte("new"), tag.Name)
	assert.Equal(t, []byte("ao-"), tag.Value)
}

var tagReplaceFnTests = []struct {
	name                   string
	params                 []string
	metaTags               test.StringTags
	seriesMetaTags         []test.StringTags
	expectedMetaTags       test.StringTags
	expectedSeriesMetaTags []test.StringTags
}{
	{
		name:                   "no tags",
		params:                 []string{"new", "a$1-", "X", "(.*)"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}},
	},
	{
		name:                   "no regex",
		params:                 []string{"new", "a$1-", "a", "woo(.*)"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}},
	},
	{
		name:                   "tag in common",
		params:                 []string{"new", "a$1-", "a", "f(.*)"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}, {N: "new", V: "aoo-"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}},
	},
	{
		name:                   "tag in common replace",
		params:                 []string{"a", "a$1-", "a", "f(.*)"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "aoo-"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}},
	},
	{
		name:                   "tag in metas, no match",
		params:                 []string{"c", "a$1-", "c", "badregex"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
	},
	{
		name:                   "tag in metas, one match",
		params:                 []string{"A", "a$1-", "c", "q(.*)"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}, {{N: "A", V: "aux-"}, {N: "c", V: "qux"}}},
	},
	{
		name:             "tag in metas, both match",
		params:           []string{"A", "a$1-", "c", "(.*)"},
		metaTags:         test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:   []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
		expectedMetaTags: test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "A", V: "abaz-"}, {N: "c", V: "baz"}},
			{{N: "A", V: "aqux-"}, {N: "c", V: "qux"}}},
	},
	{
		name:                   "tag in metas, both replace",
		params:                 []string{"c", "a$1-", "c", "(.*)"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "abaz-"}}, {{N: "c", V: "aqux-"}}},
	},
}

func TestTagReplaceFn(t *testing.T) {
	for _, tt := range tagReplaceFnTests {
		t.Run(tt.name, func(t *testing.T) {
			meta := block.Metadata{
				Tags: test.StringTagsToTags(tt.metaTags),
			}

			seriesMeta := make([]block.SeriesMeta, len(tt.seriesMetaTags))
			for i, t := range tt.seriesMetaTags {
				seriesMeta[i] = block.SeriesMeta{Tags: test.StringTagsToTags(t)}
			}

			f, err := makeTagReplaceFunc(tt.params)
			require.NoError(t, err)
			require.NotNil(t, f)
			meta, seriesMeta = f(meta, seriesMeta)

			assert.Equal(t, test.StringTagsToTags(tt.expectedMetaTags), meta.Tags)
			require.Equal(t, len(tt.expectedSeriesMetaTags), len(seriesMeta))
			for i, ex := range tt.expectedSeriesMetaTags {
				assert.Equal(t, test.StringTagsToTags(ex), seriesMeta[i].Tags)
			}
		})
	}
}

func TestTagReplaceOp(t *testing.T) {
	for _, tt := range tagReplaceFnTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewTagOp(TagReplaceType, tt.params)
			require.NoError(t, err)
			meta := block.Metadata{
				Tags: test.StringTagsToTags(tt.metaTags),
			}

			seriesMeta := make([]block.SeriesMeta, len(tt.seriesMetaTags))
			for i, t := range tt.seriesMetaTags {
				seriesMeta[i] = block.SeriesMeta{Tags: test.StringTagsToTags(t)}
			}

			bl := block.NewColumnBlockBuilder(models.NoopQueryContext(), meta, seriesMeta).Build()
			c, sink := executor.NewControllerWithSink(parser.NodeID(rune(1)))
			node := op.(baseOp).Node(c, transform.Options{})
			err = node.Process(models.NoopQueryContext(), parser.NodeID(rune(0)), bl)
			require.NoError(t, err)

			assert.Equal(t, test.StringTagsToTags(tt.expectedMetaTags), sink.Meta.Tags)
			require.Equal(t, len(tt.expectedSeriesMetaTags), len(sink.Metas))
			for i, ex := range tt.expectedSeriesMetaTags {
				assert.Equal(t, test.StringTagsToTags(ex), sink.Metas[i].Tags)
			}
		})
	}
}
