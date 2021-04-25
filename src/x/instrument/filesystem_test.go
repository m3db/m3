package instrument

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestFileSystemMetricsReport(t *testing.T) {
	testScope := tally.NewTestScope("", nil)

	every := 10 * time.Millisecond
	opts := NewOptions().SetMetricsScope(testScope).SetReportInterval(every)

	tempDir, err := ioutil.TempDir("", "fssizetest")
	assert.Nil(t, err)
	defer os.RemoveAll(tempDir)

	assert.Nil(t, os.Mkdir(filepath.Join(tempDir, "commitlogs"), 0755))
	assert.Nil(t, os.Mkdir(filepath.Join(tempDir, "snapshots"), 0755))
	assert.Nil(t, os.Mkdir(filepath.Join(tempDir, "data"), 0755))
	assert.Nil(t, os.Mkdir(filepath.Join(tempDir, "index"), 0755))

	fsReporter := NewFileSystemReporter(opts, tempDir)
	assert.Nil(t, fsReporter.Start())
	time.Sleep(1 * every)

	totalBytes, ok := testScope.Snapshot().Gauges()["filesystem.total_bytes+"]
	assert.True(t, ok)
	assert.NotEqual(t, float64(0), totalBytes.Value())

	availBytes, ok := testScope.Snapshot().Gauges()["filesystem.avail_bytes+"]
	assert.True(t, ok)
	assert.NotEqual(t, float64(0), availBytes.Value())

	numFsApiErrors, ok := testScope.Snapshot().Counters()["filesystem.num_fs_api_errors+"]
	assert.True(t, ok)
	assert.Equal(t, int64(0), numFsApiErrors.Value())

	commitLogsSize, ok := testScope.Snapshot().Gauges()["filesystem.commitlogs_bytes+"]
	assert.True(t, ok)
	assert.NotEqual(t, float64(0), commitLogsSize.Value())

	snapshotsSize, ok := testScope.Snapshot().Gauges()["filesystem.snapshots_bytes+"]
	assert.True(t, ok)
	assert.NotEqual(t, float64(0), snapshotsSize.Value())

	dataSize, ok := testScope.Snapshot().Gauges()["filesystem.data_bytes+"]
	assert.True(t, ok)
	assert.NotEqual(t, float64(0), dataSize.Value())

	indexSize, ok := testScope.Snapshot().Gauges()["filesystem.index_bytes+"]
	assert.True(t, ok)
	assert.NotEqual(t, float64(0), indexSize.Value())

	numDirApiErrors, ok := testScope.Snapshot().Counters()["filesystem.num_dir_api_errors+"]
	assert.True(t, ok)
	assert.Equal(t, int64(0), numDirApiErrors.Value())

	assert.Nil(t, fsReporter.Stop())
}
