package instrument

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestFileSystemMetricsReport(t *testing.T) {
	testScope := tally.NewTestScope("", nil)
	every := 10 * time.Millisecond
	opts := NewOptions().SetMetricsScope(testScope).SetReportInterval(every)

	fsReporter := NewFileSystemReporter(opts, "/")
	assert.Nil(t, fsReporter.Start())
	time.Sleep(1 * every)

	totalBytes, ok := testScope.Snapshot().Gauges()["filesystem.total_bytes+"]
	assert.True(t, ok)
	assert.NotEqual(t, 0, totalBytes.Value())
	availBytes, ok := testScope.Snapshot().Gauges()["filesystem.avail_bytes+"]
	assert.True(t, ok)
	assert.NotEqual(t, 0, availBytes.Value())
	numErrors, ok := testScope.Snapshot().Counters()["filesystem.num_api_errors+"]
	assert.True(t, ok)
	assert.Equal(t, int64(0), numErrors.Value())
	assert.Nil(t, fsReporter.Stop())
}
