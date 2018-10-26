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

package config

import (
	"time"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/x/cost"
	xconfig "github.com/m3db/m3x/config"
	"github.com/m3db/m3x/config/listenaddress"
	"github.com/m3db/m3x/instrument"
)

// BackendStorageType is an enum for different backends.
type BackendStorageType string

const (
	// GRPCStorageType is for backends which only support grpc endpoints.
	GRPCStorageType BackendStorageType = "grpc"
	// M3DBStorageType is for m3db backend.
	M3DBStorageType BackendStorageType = "m3db"
)

// Configuration is the configuration for the query service.
type Configuration struct {
	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// Clusters is the DB cluster configurations for read, write and
	// query endpoints.
	Clusters m3.ClustersStaticConfiguration `yaml:"clusters"`

	// LocalConfiguration is the local embedded configuration if running
	// coordinator embedded in the DB.
	Local *LocalConfiguration `yaml:"local"`

	// ClusterManagement for placemement, namespaces and database management
	// endpoints (optional).
	ClusterManagement *ClusterManagementConfiguration `yaml:"clusterManagement"`

	// ListenAddress is the server listen address.
	ListenAddress *listenaddress.Configuration `yaml:"listenAddress" validate:"nonzero"`

	// Filter is the read/write/complete tags filter configuration.
	Filter FilterConfiguration `yaml:"filter"`

	// RPC is the RPC configuration.
	RPC *RPCConfiguration `yaml:"rpc"`

	// Backend is the backend store for query service. We currently support grpc and m3db (default).
	Backend BackendStorageType `yaml:"backend"`

	// TagOptions is the tag configuration options.
	TagOptions TagOptionsConfiguration `yaml:"tagOptions"`

	// ReadWorkerPool is the worker pool policy for read requests.
	ReadWorkerPool xconfig.WorkerPoolPolicy `yaml:"readWorkerPoolPolicy"`

	// WriteWorkerPool is the worker pool policy for write requests.
	WriteWorkerPool xconfig.WorkerPoolPolicy `yaml:"writeWorkerPoolPolicy"`

	// Downsample configurates how the metrics should be downsampled.
	Downsample downsample.Configuration `yaml:"downsample"`

	// Ingest is the ingest server.
	Ingest *IngestConfiguration `yaml:"ingest"`

	// Limits specifies limits on per-query resource usage.
	Limits LimitsConfiguration `yaml:"limits"`
}

// Filter is a query filter type.
type Filter string

const (
	// FilterLocalOnly is a filter that specifies local only storage should be used.
	FilterLocalOnly Filter = "local_only"
	// FilterRemoteOnly is a filter that specifies remote only storage should be used.
	FilterRemoteOnly Filter = "remote_only"
	// FilterAllowAll is a filter that specifies all storages should be used.
	FilterAllowAll Filter = "allow_all"
	// FilterAllowNone is a filter that specifies no storages should be used.
	FilterAllowNone Filter = "allow_none"
)

// FilterConfiguration is the filters for write/read/complete tags storage filters.
type FilterConfiguration struct {
	Read         Filter `yaml:"read"`
	Write        Filter `yaml:"write"`
	CompleteTags Filter `yaml:"completeTags"`
}

// LimitsConfiguration represents limitations on resource usage in the query instance. Limits are split between per-query
// and global limits.
type LimitsConfiguration struct {
	Global   GlobalLimitsConfiguration   `yaml:"global"`
	PerQuery PerQueryLimitsConfiguration `yaml:"perQuery"`
}

// GlobalLimitsConfiguration represents limits on resource usage across a query instance. Zero or negative values imply no limit.
type GlobalLimitsConfiguration struct {

	// MaxFetchedDatapoints limits the total number of datapoints actually fetched by all queries at any given time.
	MaxFetchedDatapoints int64 `yaml:"maxFetchedDatapoints"`
}

// AsLimitManagerOptions converts this configuration to cost.LimitManagerOptions for MaxFetchedDatapoints.
func (l *GlobalLimitsConfiguration) AsLimitManagerOptions() cost.LimitManagerOptions {
	return toLimitManagerOptions(l.MaxFetchedDatapoints)
}

// PerQueryLimitsConfiguration represents limits on resource usage within a single query. Zero or negative values imply no limit.
type PerQueryLimitsConfiguration struct {

	// MaxComputedDatapoints limits the number of datapoints that can be returned by a query. It's determined purely
	// from the size of the time range and the step size (end - start / step).
	MaxComputedDatapoints int64 `yaml:"maxComputedDatapoints"`

	// MaxFetchedDatapoints limits the number of datapoints actually used by a given query.
	MaxFetchedDatapoints int64 `yaml:"maxFetchedDatapoints"`
}

// AsLimitManagerOptions converts this configuration to cost.LimitManagerOptions for MaxFetchedDatapoints.
func (l *PerQueryLimitsConfiguration) AsLimitManagerOptions() cost.LimitManagerOptions {
	return toLimitManagerOptions(l.MaxFetchedDatapoints)
}

func toLimitManagerOptions(limit int64) cost.LimitManagerOptions {
	return cost.NewLimitManagerOptions().SetDefaultLimit(cost.Limit{
		Threshold: cost.Cost(limit),
		Enabled:   limit > 0,
	})
}

// IngestConfiguration is the configuration for ingestion server.
type IngestConfiguration struct {
	// Ingester is the configuration for storage based ingester.
	Ingester ingest.Configuration `yaml:"ingester"`

	// M3Msg is the configuration for m3msg server.
	M3Msg m3msg.Configuration `yaml:"m3msg"`
}

// LocalConfiguration is the local embedded configuration if running
// coordinator embedded in the DB.
type LocalConfiguration struct {
	// Namespace is the name of the local namespace to write/read from.
	Namespace string `yaml:"namespace" validate:"nonzero"`

	// Retention is the retention of the local namespace to write/read from.
	Retention time.Duration `yaml:"retention" validate:"nonzero"`
}

// ClusterManagementConfiguration is configuration for the placemement,
// namespaces and database management endpoints (optional).
type ClusterManagementConfiguration struct {
	// Etcd is the client configuration for etcd.
	Etcd etcdclient.Configuration `yaml:"etcd"`
}

// RPCConfiguration is the RPC configuration for the coordinator for
// the GRPC server used for remote coordinator to coordinator calls.
type RPCConfiguration struct {
	// Enabled determines if coordinator RPC is enabled for remote calls.
	Enabled bool `yaml:"enabled"`

	// ListenAddress is the RPC server listen address.
	ListenAddress string `yaml:"listenAddress"`

	// RemoteListenAddresses is the remote listen addresses to call for remote
	// coordinator calls.
	RemoteListenAddresses []string `yaml:"remoteListenAddresses"`
}

// TagOptionsConfiguration is the configuration for shared tag options
// Currently only name, but can expand to cover deduplication settings, or other
// relevant options.
type TagOptionsConfiguration struct {
	// MetricName specifies the tag name that corresponds to the metric's name tag
	// If not provided, defaults to `__name__`
	MetricName string `yaml:"metricName"`
}

// TagOptionsFromConfig translates tag option configuration into tag options.
func TagOptionsFromConfig(cfg TagOptionsConfiguration) (models.TagOptions, error) {
	opts := models.NewTagOptions()
	name := cfg.MetricName
	if name != "" {
		opts = opts.SetMetricName([]byte(name))
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return opts, nil
}
