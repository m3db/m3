package instrument

import (
	"math"
	"path/filepath"
	"time"

	xos "github.com/m3db/m3/src/x/os"
	"github.com/uber-go/tally"
)

const (
	// report interval (default 1s) is extended for this reporter
	// to keep the reporting overhead low
	minFileSystemReportInterval = float64(30)
	commitlogsDir               = "commitlogs"
	snapshotsDir                = "snapshots"
	dataDir                     = "data"
	indexDir                    = "index"
)

type fileSystemReporter struct {
	baseReporter

	metrics fileSystemMetrics
}

type fileSystemMetrics struct {
	fsTotal  tally.Gauge   // total space in the filesystem in bytes
	fsAvail  tally.Gauge   // available space in the filesystem in bytes
	fsErrors tally.Counter // errors in the filesystems api

	commitlogsSize tally.Gauge   // size of the commitlogs directory in bytes
	snapshotsSize  tally.Gauge   // size of the snapshots directory in bytes
	dataSize       tally.Gauge   // size of the data directory in bytes
	indexSize      tally.Gauge   // size of the index directory in bytes
	dirErrors      tally.Counter // errors in the directory size api

	rootDir string
	opts    Options
}

func (r *fileSystemMetrics) report() {
	fsStats, err := xos.GetFileSystemStats(r.rootDir)
	if err != nil {
		r.fsErrors.Inc(1)
	} else {
		r.fsTotal.Update(float64(fsStats.Total))
		r.fsAvail.Update(float64(fsStats.Avail))
	}

	commitlogsDirStats, err := xos.GetDirectoryStats(filepath.Join(r.rootDir, commitlogsDir))
	if err != nil {
		r.dirErrors.Inc(1)
	} else {
		r.commitlogsSize.Update(float64(commitlogsDirStats.Size))
	}
	snapshotsStats, err := xos.GetDirectoryStats(filepath.Join(r.rootDir, snapshotsDir))
	if err != nil {
		r.dirErrors.Inc(1)
	} else {
		r.commitlogsSize.Update(float64(snapshotsStats.Size))
	}
	dataStats, err := xos.GetDirectoryStats(filepath.Join(r.rootDir, dataDir))
	if err != nil {
		r.dirErrors.Inc(1)
	} else {
		r.commitlogsSize.Update(float64(dataStats.Size))
	}
	indexStats, err := xos.GetDirectoryStats(filepath.Join(r.rootDir, indexDir))
	if err != nil {
		r.dirErrors.Inc(1)
	} else {
		r.commitlogsSize.Update(float64(indexStats.Size))
	}
}

func NewFileSystemReporter(
	opts Options,
	rootDir string,
) Reporter {
	fileSystemScope := opts.MetricsScope().SubScope("filesystem")

	r := &fileSystemReporter{
		metrics: fileSystemMetrics{
			fsTotal:  fileSystemScope.Gauge("total_bytes"),
			fsAvail:  fileSystemScope.Gauge("avail_bytes"),
			fsErrors: fileSystemScope.Counter("num_fs_api_errors"),

			commitlogsSize: fileSystemScope.Gauge("commitlogs_bytes"),
			snapshotsSize:  fileSystemScope.Gauge("snapshots_bytes"),
			dataSize:       fileSystemScope.Gauge("data_bytes"),
			indexSize:      fileSystemScope.Gauge("index_bytes"),
			dirErrors:      fileSystemScope.Counter("num_dir_api_errors"),

			rootDir: rootDir,
			opts:    opts,
		},
	}

	reportInterval := time.Duration(math.Max(opts.ReportInterval().Seconds(), minFileSystemReportInterval))
	r.init(reportInterval, r.metrics.report)
	return r
}
