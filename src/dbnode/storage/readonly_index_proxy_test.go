// Copyright (c) 2020 Uber Technologies, Inc.
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

package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestReadOnlyIndexProxyReject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	idx := NewMockNamespaceIndex(ctrl)
	roIdx := NewReadOnlyIndexProxy(idx)

	assert.Equal(t, errNamespaceIndexReadOnly, roIdx.WriteBatch(nil))
	assert.Equal(t, errNamespaceIndexReadOnly, roIdx.WritePending(nil))
}

func TestReadOnlyIndexProxySuppress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	idx := NewMockNamespaceIndex(ctrl)
	roIdx := NewReadOnlyIndexProxy(idx)

	roIdx.AssignShardSet(nil)

	assert.NoError(t, roIdx.Bootstrap(nil))

	assert.NoError(t, roIdx.CleanupExpiredFileSets(time.Now()))

	assert.NoError(t, roIdx.CleanupDuplicateFileSets())

	res, err := roIdx.Tick(nil, time.Now())
	assert.Equal(t, namespaceIndexTickResult{}, res)
	assert.NoError(t, err)

	_, err = roIdx.WarmFlush(nil, nil)
	assert.NoError(t, err)

	_, err = roIdx.ColdFlush(nil)
	assert.NoError(t, err)

	assert.NoError(t, roIdx.Close())
}

func TestReadOnlyIndexProxyDelegate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	idx := NewMockNamespaceIndex(ctrl)
	roIdx := NewReadOnlyIndexProxy(idx)

	now := time.Now().Truncate(time.Hour)
	later := xtime.ToUnixNano(now.Add(time.Hour))
	testErr := errors.New("test error")

	idx.EXPECT().BlockStartForWriteTime(now).Return(later)
	assert.Equal(t, later, roIdx.BlockStartForWriteTime(now))

	block := index.NewMockBlock(ctrl)
	idx.EXPECT().BlockForBlockStart(now).Return(block, testErr)
	res, err := roIdx.BlockForBlockStart(now)
	assert.Equal(t, testErr, err)
	assert.Equal(t, block, res)

	ctx := context.NewContext()
	query := index.Query{}
	queryOpts := index.QueryOptions{}
	queryRes := index.QueryResult{}

	idx.EXPECT().Query(ctx, query, queryOpts).Return(queryRes, testErr)
	qRes, err := roIdx.Query(ctx, query, queryOpts)
	assert.Equal(t, testErr, err)
	assert.Equal(t, queryRes, qRes)

	aggOpts := index.AggregationOptions{}
	aggRes := index.AggregateQueryResult{}
	idx.EXPECT().AggregateQuery(ctx, query, aggOpts).Return(aggRes, testErr)
	aRes, err := roIdx.AggregateQuery(ctx, query, aggOpts)
	assert.Equal(t, testErr, err)
	assert.Equal(t, aggRes, aRes)

	wideOpts := index.WideQueryOptions{}
	ch := make(chan *ident.IDBatch)
	idx.EXPECT().WideQuery(ctx, query, ch, wideOpts).Return(testErr)
	err = roIdx.WideQuery(ctx, query, ch, wideOpts)
	assert.Equal(t, testErr, err)
	close(ch)

	idx.EXPECT().Bootstrapped().Return(true)
	assert.True(t, roIdx.Bootstrapped())

	idx.EXPECT().SetExtendedRetentionPeriod(time.Minute)
	roIdx.SetExtendedRetentionPeriod(time.Minute)

	debugOpts := DebugMemorySegmentsOptions{}
	idx.EXPECT().DebugMemorySegments(debugOpts).Return(testErr)
	assert.Equal(t, testErr, roIdx.DebugMemorySegments(debugOpts))
}
