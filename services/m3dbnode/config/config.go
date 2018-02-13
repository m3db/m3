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

package config

import (
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/environment"
	"github.com/m3db/m3x/config/hostid"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
)

// Configuration is the configuration for a M3DB node.
type Configuration struct {
	// Logging configuration.
	Logging xlog.Configuration `yaml:"logging"`

	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// The host and port on which to listen for the node service.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// The host and port on which to listen for the cluster service.
	ClusterListenAddress string `yaml:"clusterListenAddress" validate:"nonzero"`

	// The HTTP host and port on which to listen for the node service.
	HTTPNodeListenAddress string `yaml:"httpNodeListenAddress" validate:"nonzero"`

	// The HTTP host and port on which to listen for the cluster service.
	HTTPClusterListenAddress string `yaml:"httpClusterListenAddress" validate:"nonzero"`

	// The host and port on which to listen for debug endpoints.
	DebugListenAddress string `yaml:"debugListenAddress"`

	// HostID is the local host ID configuration.
	HostID hostid.Configuration `yaml:"hostID"`

	// Client configuration, used for inter-node communication and when used as a coordinator.
	Client client.Configuration `yaml:"client"`

	// The initial garbage collection target percentage.
	GCPercentage int `yaml:"gcPercentage" validate:"max=100"`

	// Write new series limit per second to limit overwhelming during new ID bursts.
	WriteNewSeriesLimitPerSecond int `yaml:"writeNewSeriesLimitPerSecond"`

	// Write new series backoff between batches of new series insertions.
	WriteNewSeriesBackoffDuration time.Duration `yaml:"writeNewSeriesBackoffDuration"`

	// The tick configuration, omit this to use default settings.
	Tick *TickConfiguration `yaml:"tick"`

	// Bootstrap configuration.
	Bootstrap BootstrapConfiguration `yaml:"bootstrap"`

	// The block retriever policy.
	BlockRetrieve *BlockRetrievePolicy `yaml:"blockRetrieve"`

	// Cache configurations.
	Cache CacheConfigurations `yaml:"cache"`

	// The filesystem configuration for the node.
	Filesystem FilesystemConfiguration `yaml:"fs"`

	// The commit log policy for the node.
	CommitLog CommitLogPolicy `yaml:"commitlog"`

	// The repair policy for repairing in-memory data.
	Repair RepairPolicy `yaml:"repair"`

	// The pooling policy.
	PoolingPolicy PoolingPolicy `yaml:"pooling"`

	// The environment (static or dynamic) configuration.
	EnvironmentConfig environment.Configuration `yaml:"config"`

	// The configuration for hashing
	Hashing HashingConfiguration `yaml:"hashing"`

	// Write new series asynchronously for fast ingestion of new ID bursts.
	WriteNewSeriesAsync bool `yaml:"writeNewSeriesAsync"`
}

// TickConfiguration is the tick configuration for background processing of
// series as blocks are rotated from mutable to immutable and out of order
// writes are merged.
type TickConfiguration struct {
	// Tick series batch size is the batch size to process series together
	// during a tick before yielding and sleeping the per series duration
	// multiplied by the batch size.
	// The higher this value is the more variable CPU utilization will be
	// but the shorter ticks will ultimately be.
	SeriesBatchSize int `yaml:"seriesBatchSize"`

	// Tick per series sleep at the completion of a tick batch.
	PerSeriesSleepDuration time.Duration `yaml:"perSeriesSleepDuration"`

	// Tick minimum interval controls the minimum tick interval for the node.
	MinimumInterval time.Duration `yaml:"minimumInterval"`
}

// BlockRetrievePolicy is the block retrieve policy.
type BlockRetrievePolicy struct {
	// FetchConcurrency is the concurrency to fetch blocks from disk. For
	// spinning disks it is highly recommended to set this value to 1.
	FetchConcurrency int `yaml:"fetchConcurrency" validate:"min=0"`
}

// CommitLogPolicy is the commit log policy.
type CommitLogPolicy struct {
	// The max size the commit log will flush a segment to disk after buffering.
	FlushMaxBytes int `yaml:"flushMaxBytes" validate:"nonzero"`

	// The maximum amount of time the commit log will wait to flush to disk.
	FlushEvery time.Duration `yaml:"flushEvery" validate:"nonzero"`

	// The queue the commit log will keep in front of the current commit log segment.
	Queue CommitLogQueuePolicy `yaml:"queue" validate:"nonzero"`

	// The commit log retention policy.
	RetentionPeriod time.Duration `yaml:"retentionPeriod" validate:"nonzero"`

	// The commit log block size.
	BlockSize time.Duration `yaml:"blockSize" validate:"nonzero"`
}

// CalculationType is a type of configuration parameter.
type CalculationType string

const (
	// CalculationTypeFixed is a fixed parameter not to be scaled of any parameter.
	CalculationTypeFixed CalculationType = "fixed"
	// CalculationTypePerCPU is a parameter that needs to be scaled by number of CPUs.
	CalculationTypePerCPU CalculationType = "percpu"
)

// CommitLogQueuePolicy is the commit log queue policy.
type CommitLogQueuePolicy struct {
	// The type of calculation for the size.
	CalculationType CalculationType `yaml:"calculationType"`

	// The size of the commit log, calculated according to the calculation type.
	Size int `yaml:"size" validate:"nonzero"`
}

// RepairPolicy is the repair policy.
type RepairPolicy struct {
	// Enabled or disabled.
	Enabled bool `yaml:"enabled"`

	// The repair interval.
	Interval time.Duration `yaml:"interval" validate:"nonzero"`

	// The repair time offset.
	Offset time.Duration `yaml:"offset" validate:"nonzero"`

	// The repair time jitter.
	Jitter time.Duration `yaml:"jitter" validate:"nonzero"`

	// The repair throttle.
	Throttle time.Duration `yaml:"throttle" validate:"nonzero"`

	// The repair check interval.
	CheckInterval time.Duration `yaml:"checkInterval" validate:"nonzero"`
}

// HashingConfiguration is the configuration for hashing.
type HashingConfiguration struct {
	// Murmur32 seed value.
	Seed uint32 `yaml:"seed"`
}
