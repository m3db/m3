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

	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
)

// Configuration is the configuration for a M3DB node.
type Configuration struct {
	// Logging configuration.
	Logging xlog.Configuration `yaml:"logging"`

	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// The host and port on which to listen for the node service
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// The host and port on which to listen for the cluster service
	ClusterListenAddress string `yaml:"clusterListenAddress" validate:"nonzero"`

	// The HTTP host and port on which to listen for the node service
	HTTPNodeListenAddress string `yaml:"httpNodeListenAddress" validate:"nonzero"`

	// The HTTP host and port on which to listen for the cluster service
	HTTPClusterListenAddress string `yaml:"httpClusterListenAddress" validate:"nonzero"`

	// The host and port on which to listen for debug endpoints
	DebugListenAddress string `yaml:"debugListenAddress"`

	// HostID is the local host ID configuration.
	HostID HostIDConfiguration `yaml:"hostID"`

	// Client configuration, used for inter-node communication and when used as a coordinator.
	Client client.Configuration `yaml:"client"`

	// The initial garbage collection target percentage
	GCPercentage int `yaml:"gcPercentage" validate:"max=100"`

	// Write new series asynchronously for fast ingestion of new ID bursts
	WriteNewSeriesAsync bool `yaml:"writeNewSeriesAsync"`

	// Write new series limit per second to limit overwhelming during new ID bursts
	WriteNewSeriesLimitPerSecond int `yaml:"writeNewSeriesLimitPerSecond"`

	// Write new series backoff between batches of new series insertions
	WriteNewSeriesBackoffDuration time.Duration `yaml:"writeNewSeriesBackoffDuration"`

	// Bootstrap configuration
	Bootstrap BootstrapConfiguration `yaml:"bootstrap"`

	// The filesystem configuration for the node
	Filesystem FilesystemConfiguration `yaml:"fs"`

	// TickInterval controls the tick interval for the node
	TickInterval time.Duration `yaml:"tickInterval" validate:"nonzero"`

	// The commit log policy for the node
	CommitLog commitLogPolicy `yaml:"commitlog"`

	// The repair policy for repairing in-memory data
	Repair repairPolicy `yaml:"repair"`

	// The pooling policy
	PoolingPolicy PoolingPolicy `yaml:"poolingPolicy"`

	// The configuration for config service client
	ConfigService etcdclient.Configuration `yaml:"configService"`
}

type commitLogPolicy struct {
	// The max size the commit log will flush a segment to disk after buffering
	FlushMaxBytes int `yaml:"flushMaxBytes" validate:"nonzero"`

	// The maximum amount of time the commit log will wait to flush to disk
	FlushEvery time.Duration `yaml:"flushEvery" validate:"nonzero"`

	// The queue the commit log will keep in front of the current commit log segment
	Queue commitLogQueuePolicy `yaml:"queue" validate:"nonzero"`

	// The commit log retention policy
	RetentionPeriod time.Duration `yaml:"retentionPeriod" validate:"nonzero"`

	// The commit log block size
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

type commitLogQueuePolicy struct {
	// The type of calculation for the size
	CalculationType CalculationType `yaml:"calculationType"`

	// The size of the commit log, calculated according to the calculation type
	Size int `yaml:"size" validate:"nonzero"`
}

type repairPolicy struct {
	// Enabled or disabled
	Enabled bool `yaml:"enabled"`

	// The repair interval
	Interval time.Duration `yaml:"interval" validate:"nonzero"`

	// The repair time offset
	Offset time.Duration `yaml:"offset" validate:"nonzero"`

	// The repair time jitter
	Jitter time.Duration `yaml:"jitter" validate:"nonzero"`

	// The repair throttle
	Throttle time.Duration `yaml:"throttle" validate:"nonzero"`

	// The repair check interval
	CheckInterval time.Duration `yaml:"checkInterval" validate:"nonzero"`
}
