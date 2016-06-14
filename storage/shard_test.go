package storage

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/mocks"

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

func testDatabaseShard(opts memtsdb.DatabaseOptions) *dbShard {
	return newDatabaseShard(0, opts).(*dbShard)
}

func TestShardFlushToDiskDuringBootstrap(t *testing.T) {
	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapping
	err := s.FlushToDisk(time.Now())
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

	require.Nil(t, s.FlushToDisk(blockStart))
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
		series.EXPECT().FlushToDisk(s.flushWriter, blockStart, gomock.Any()).Do(func(_ interface{}, _ time.Time, _ [][]byte) {
			flushed[i] = struct{}{}
		}).Return(expectedErr)
		s.list.PushBack(series)
	}
	s.FlushToDisk(blockStart)

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}
}
