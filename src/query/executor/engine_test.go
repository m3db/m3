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

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEngine(
	s storage.Storage,
	lookbackDuration time.Duration,
	instrumentOpts instrument.Options,
) Engine {
	engineOpts := NewEngineOptions().
		SetStore(s).
		SetLookbackDuration(lookbackDuration).
		SetInstrumentOptions(instrumentOpts)

	return NewEngine(engineOpts)
}

func TestExecute(t *testing.T) {
	ctrl := xtest.NewController(t)
	store, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any()).Return(nil, client.FetchResponseMetadata{Exhaustive: false}, fmt.Errorf("dummy"))
	session.EXPECT().IteratorPools().Return(nil, nil)

	// Results is closed by execute
	engine := newEngine(store, time.Minute, instrument.NewOptions())
	_, err := engine.ExecuteProm(context.TODO(),
		&storage.FetchQuery{}, &QueryOptions{}, storage.NewFetchOptions())
	assert.NotNil(t, err)
}

func TestExecuteExpr(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	parser, err := promql.Parse("foo", time.Second,
		models.NewTagOptions(), promql.NewParseOptions())
	require.NoError(t, err)

	store := storage.NewMockStorage(ctrl)
	store.EXPECT().FetchBlocks(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(block.Result{
			Blocks: []block.Block{block.NewMockBlock(ctrl)},
		}, nil)
	engine := newEngine(store, defaultLookbackDuration,
		instrument.NewOptions())
	_, err = engine.ExecuteExpr(context.TODO(), parser,
		&QueryOptions{}, storage.NewFetchOptions(), models.RequestParams{
			Start: xtime.Now().Add(-2 * time.Second),
			End:   xtime.Now(),
			Step:  time.Second,
		})

	require.NoError(t, err)
}
