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

package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/storage/index"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNamespaceIndexCleanupExpiredFilesets(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	now := time.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	oldestTime := now.Add(-time.Hour * 8)
	files := []string{"abc"}

	idx.indexFilesetsBeforeFn = func(dir string, nsID ident.ID, exclusiveTime time.Time) ([]string, error) {
		require.True(t, oldestTime.Equal(exclusiveTime), fmt.Sprintf("%v %v", exclusiveTime, oldestTime))
		return files, nil
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Equal(t, files, s)
		return nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}

func TestNamespaceIndexCleanupExpiredFilesetsWithBlocks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	now := time.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	oldestTime := now.Add(-time.Hour * 9)
	idx.state.blocksByTime[xtime.ToUnixNano(oldestTime)] = mockBlock

	idx.indexFilesetsBeforeFn = func(dir string, nsID ident.ID, exclusiveTime time.Time) ([]string, error) {
		require.True(t, exclusiveTime.Equal(oldestTime))
		return nil, nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}
