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

package executor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestEngine_Execute(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	store, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, false, fmt.Errorf("dummy"))
	session.EXPECT().IteratorPools().Return(nil, nil)

	// Results is closed by execute
	results := make(chan *storage.QueryResult)
	engine := NewEngine(store, tally.NewTestScope("test", nil), nil)
	go engine.Execute(context.TODO(), &storage.FetchQuery{}, &EngineOptions{}, results)
	res := <-results
	assert.NotNil(t, res.Err)
}

func TestEngine_ExecuteExpr(t *testing.T) {
	t.Run("releases and reports on completion", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockEnforcer := cost.NewMockChainedEnforcer(ctrl)
		mockEnforcer.EXPECT().Release().Times(1)

		mockParent := cost.NewMockChainedEnforcer(ctrl)
		mockParent.EXPECT().Child(gomock.Any()).Return(mockEnforcer)

		parser, err := promql.Parse("foo", models.NewTagOptions())
		require.NoError(t, err)

		results := make(chan Query)
		engine := NewEngine(mock.NewMockStorage(), tally.NewTestScope("", nil), mockParent)
		go engine.ExecuteExpr(context.TODO(), parser, &EngineOptions{}, models.RequestParams{
			Start: time.Now().Add(-2 * time.Second),
			End:   time.Now(),
			Step:  time.Second,
		}, results)

		// drain the channel
		res := <-results

		require.NoError(t, res.Err)
	})
}
