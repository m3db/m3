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

package block

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3db/ts"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockRetrieveManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBlockRetriever := NewMockDatabaseBlockRetriever(ctrl)

	called := 0
	mgr := NewDatabaseBlockRetrieverManager(func(
		namespace ts.ID,
	) (DatabaseBlockRetriever, error) {
		called++
		assert.Equal(t, "testns", namespace.String())
		return mockBlockRetriever, nil
	})

	r, err := mgr.Retriever(ts.StringID("testns"))
	require.NoError(t, err)

	assert.Equal(t, DatabaseBlockRetriever(mockBlockRetriever), r)

	r2, err := mgr.Retriever(ts.StringID("testns"))
	require.NoError(t, err)

	assert.Equal(t, r, r2)
	assert.Equal(t, 1, called)
}

func TestBlockRetrieveManagerError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBlockRetriever := NewMockDatabaseBlockRetriever(ctrl)

	called := 0
	expectedErr := fmt.Errorf("expected error")
	mgr := NewDatabaseBlockRetrieverManager(func(
		namespace ts.ID,
	) (DatabaseBlockRetriever, error) {
		defer func() { called++ }()
		if called == 0 {
			return nil, expectedErr
		}
		assert.Equal(t, "testns", namespace.String())
		return mockBlockRetriever, nil
	})

	r, err := mgr.Retriever(ts.StringID("testns"))
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	require.Nil(t, r)

	r, err = mgr.Retriever(ts.StringID("testns"))
	require.NoError(t, err)

	assert.Equal(t, DatabaseBlockRetriever(mockBlockRetriever), r)

	assert.Equal(t, 2, called)
}

func TestDatabaseShardBlockRetrieverManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBlockRetriever := NewMockDatabaseBlockRetriever(ctrl)

	mgr := NewDatabaseShardBlockRetrieverManager(mockBlockRetriever)

	id := ts.StringID("foo")
	blockStart := time.Now().Truncate(2 * time.Hour)

	mockBlockRetriever.
		EXPECT().
		Stream(uint32(42), ts.NewIDMatcher("foo"), blockStart, nil).
		Times(2)

	mgr.ShardRetriever(42).Stream(id, blockStart, nil)
	mgr.ShardRetriever(42).Stream(id, blockStart, nil)

	mockBlockRetriever.
		EXPECT().
		Stream(uint32(3), ts.NewIDMatcher("foo"), blockStart, nil)

	mgr.ShardRetriever(3).Stream(id, blockStart, nil)
}
