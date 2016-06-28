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

package storage

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func createShardDir(t *testing.T) (string, string) {
	dir, err := ioutil.TempDir("", "foo")
	require.NoError(t, err)
	shardDir := path.Join(dir, "0")
	err = os.Mkdir(shardDir, os.ModeDir|os.FileMode(0755))
	require.NoError(t, err)
	return dir, shardDir
}

func openFile(t *testing.T, filePath string) *os.File {
	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	require.NoError(t, err)
	return fd
}

func testDatabaseShard(opts m3db.DatabaseOptions) *dbShard {
	return newDatabaseShard(0, opts).(*dbShard)
}

func TestShardFlushToDiskDuringBootstrap(t *testing.T) {
	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapping
	err := s.FlushToDisk(nil, time.Now())
	require.Equal(t, err, errShardNotBootstrapped)
}

func TestShardFlushToDiskFileExists(t *testing.T) {
	dir, shardDir := createShardDir(t)
	defer os.RemoveAll(dir)

	opts := testDatabaseOptions().FilePathPrefix(dir)
	s := testDatabaseShard(opts)
	s.bs = bootstrapped

	blockStart := time.Unix(21600, 0)
	checkpointFile := path.Join(shardDir, "21600000000000-checkpoint.db")
	fd := openFile(t, checkpointFile)
	fd.Close()

	require.Nil(t, s.FlushToDisk(nil, blockStart))
}

func TestShardFlushToDiskSeriesFlushError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir, _ := createShardDir(t)
	defer os.RemoveAll(dir)

	opts := testDatabaseOptions().FilePathPrefix(dir)
	s := testDatabaseShard(opts)
	s.bs = bootstrapped

	blockStart := time.Unix(21600, 0)
	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		var expectedErr error
		if i == 1 {
			expectedErr = errors.New("some error")
		}
		series := mocks.NewMockdatabaseSeries(ctrl)
		series.EXPECT().FlushToDisk(nil, s.flushWriter, blockStart, gomock.Any()).Do(func(_ m3db.Context, _ interface{}, _ time.Time, _ [][]byte) {
			flushed[i] = struct{}{}
		}).Return(expectedErr)
		s.list.PushBack(series)
	}
	s.FlushToDisk(nil, blockStart)

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}
}
