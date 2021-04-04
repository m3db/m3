package instrument

import (
	"time"

	xos "github.com/m3db/m3/src/x/os"
	"github.com/uber-go/tally"
)

const (
	// report interval (default 1s) is extended for this reporter
	// to keep the reporting overhead low
	fileSystemReportMultiplier = 30
)

type fileSystemReporter struct {
	baseReporter

	metrics fileSystemMetrics
}

type fileSystemMetrics struct {
	Total  tally.Gauge // total space in the filesystem in bytes
	Avail  tally.Gauge // available space in the filesystem in bytes
	Errors tally.Counter

	rootDir string
	opts    Options
}

func (r *fileSystemMetrics) report() {
	info, err := xos.GetFileSystemStats(r.rootDir)
	if err != nil {
		r.Errors.Inc(1)
	}
	r.Total.Update(float64(info.Total))
	r.Avail.Update(float64(info.Avail))
}

func NewFileSystemReporter(
	opts Options,
	rootDir string,
) Reporter {
	fileSystemScope := opts.MetricsScope().SubScope("filesystem")

	r := &fileSystemReporter{
		metrics: fileSystemMetrics{
			Total:   fileSystemScope.Gauge("total_bytes"),
			Avail:   fileSystemScope.Gauge("avail_bytes"),
			Errors:  fileSystemScope.Counter("num_api_errors"),
			rootDir: rootDir,
			opts:    opts,
		},
	}
	reportInterval := time.Duration(opts.ReportInterval() * fileSystemReportMultiplier)
	r.init(reportInterval, r.metrics.report)
	return r
}
