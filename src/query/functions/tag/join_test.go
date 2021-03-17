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

func TestCombineSingleTagWithSeparator(t *testing.T) {
	name := []byte("foo")
	sep := []byte("@-!-@")
	vals := [][]byte{
		[]byte("a"),
	}

	combined := combineTagsWithSeparator(name, sep, vals)
	expected := models.Tag{Name: name, Value: []byte("a")}
	assert.Equal(t, expected, combined)
}

func TestCombineTagsWithSeparator(t *testing.T) {
	name := []byte("foo")
	sep := []byte("-!-")
	vals := [][]byte{
		[]byte("a"),
		[]byte("bcd"),
		[]byte(""),
		[]byte("efghijkl"),
	}

	combined := combineTagsWithSeparator(name, sep, vals)
	expected := models.Tag{Name: name, Value: []byte("a-!-bcd-!--!-efghijkl")}
	assert.Equal(t, expected, combined)
}

var tagJoinFnTests = []struct {
	name                   string
	params                 []string
	metaTags               test.StringTags
	seriesMetaTags         []test.StringTags
	expectedMetaTags       test.StringTags
	expectedSeriesMetaTags []test.StringTags
}{
	{
		name:                   "no tag matchers",
		params:                 []string{"n", "-"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}},
	},
	{
		name:                   "no tags",
		params:                 []string{"n", "-", "x", "y"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}},
	},
	{
		name:                   "only common",
		params:                 []string{"n", "-", "a", "b"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}, {N: "n", V: "foo-bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}},
	},
	{
		name:                   "duplicate",
		params:                 []string{"n", "-", "b", "a", "a", "a"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}, {N: "n", V: "bar-foo-foo-foo"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}}},
	},
	{
		name:                   "only series metas",
		params:                 []string{"n", "-", "c", "c"},
		metaTags:               test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:         []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
		expectedMetaTags:       test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{{{N: "c", V: "baz"}, {N: "n", V: "baz-baz"}}, {{N: "c", V: "qux"}, {N: "n", V: "qux-qux"}}},
	},
	{
		name:             "mixed",
		params:           []string{"aa", "!", "c", "a", "b"},
		metaTags:         test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:   []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
		expectedMetaTags: test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{
			{{N: "aa", V: "baz!foo!bar"}, {N: "c", V: "baz"}},
			{{N: "aa", V: "qux!foo!bar"}, {N: "c", V: "qux"}},
		},
	},
	{
		name:             "mixed with duplicates",
		params:           []string{"aa", "!", "c", "a", "b", "c", "a", "c"},
		metaTags:         test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:   []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
		expectedMetaTags: test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{
			{{N: "aa", V: "baz!foo!bar!baz!foo!baz"}, {N: "c", V: "baz"}},
			{{N: "aa", V: "qux!foo!bar!qux!foo!qux"}, {N: "c", V: "qux"}},
		},
	},
	{
		name:             "mixed with replace",
		params:           []string{"c", "!", "c", "a", "b"},
		metaTags:         test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		seriesMetaTags:   []test.StringTags{{{N: "c", V: "baz"}}, {{N: "c", V: "qux"}}},
		expectedMetaTags: test.StringTags{{N: "a", V: "foo"}, {N: "b", V: "bar"}},
		expectedSeriesMetaTags: []test.StringTags{
			{{N: "c", V: "baz!foo!bar"}},
			{{N: "c", V: "qux!foo!bar"}},
		},
	},
}

func TestTagJoinFn(t *testing.T) {
	for _, tt := range tagJoinFnTests {
		t.Run(tt.name, func(t *testing.T) {
			meta := block.Metadata{
				Tags: test.StringTagsToTags(tt.metaTags),
			}

			seriesMeta := make([]block.SeriesMeta, len(tt.seriesMetaTags))
			for i, t := range tt.seriesMetaTags {
				seriesMeta[i] = block.SeriesMeta{Tags: test.StringTagsToTags(t)}
			}

			f, err := makeTagJoinFunc(tt.params)
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

func TestTagJoinOp(t *testing.T) {
	for _, tt := range tagJoinFnTests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewTagOp(TagJoinType, tt.params)
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
