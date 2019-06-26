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

package functions

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetch(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	b := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	mockStorage := mock.NewMockStorage()
	mockStorage.SetFetchBlocksResult(block.Result{Blocks: []block.Block{b}}, nil)
	source := (&FetchOp{}).Node(c, mockStorage, transform.Options{})
	err := source.Execute(models.NoopQueryContext())
	require.NoError(t, err)
	expected := values
	assert.Len(t, sink.Values, 2)
	assert.Equal(t, expected, sink.Values)
}

type predicateMatcher struct {
	name string
	fn   func(interface{}) bool
}

var _ gomock.Matcher = &predicateMatcher{}

func (m *predicateMatcher) Matches(i interface{}) bool {
	return m.fn(i)
}

func (m *predicateMatcher) String() string {
	return m.name
}

func TestOffsetFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)
	op := &FetchOp{
		Offset: time.Minute,
	}

	now := time.Now()
	start := now.Add(time.Hour * -1)
	opts := test.TransformOptions(t, transform.OptionsParams{
		TimeSpec: transform.TimeSpec{
			Start: start,
			End:   now,
			Now:   now,
		},
	})

	qMatcher := &predicateMatcher{
		name: "query",
		fn: func(i interface{}) bool {
			q, ok := i.(*storage.FetchQuery)
			if !ok {
				return false
			}

			return q.Start.Equal(start.Add(time.Minute*-1)) &&
				q.End.Equal(now.Add(time.Minute*-1))
		},
	}

	optsMatcher := &predicateMatcher{
		name: "opts",
		fn: func(i interface{}) bool {
			_, ok := i.(*storage.FetchOptions)
			return ok
		},
	}

	store.EXPECT().FetchBlocks(gomock.Any(), qMatcher, optsMatcher)

	c, _ := executor.NewControllerWithSink(parser.NodeID(1))
	node := op.Node(c, store, opts)

	err := node.Execute(models.NoopQueryContext())
	require.NoError(t, err)
}
