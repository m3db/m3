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
	"errors"
	"fmt"
	"time"

	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	ingestm3msg "github.com/m3db/m3/src/cmd/services/m3coordinator/ingest/m3msg"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/x/cost"
	xdocs "github.com/m3db/m3/src/x/docs"
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

	defaultCarbonIngesterListenAddress = "0.0.0.0:7204"
	errNoIDGenerationScheme            = "error: a recent breaking change means that an ID " +
		"generation scheme is required in coordinator configuration settings. " +
		"More information is available here: %s"

	defaultQueryConversionCacheSize = 4096
)

var (
	// 5m is the default lookback in Prometheus
	defaultLookbackDuration = 5 * time.Minute

	defaultCarbonIngesterAggregationType = aggregation.Mean
)

// Configuration is the configuration for the query service.
type Configuration struct {
	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// Tracing configures opentracing. If not provided, tracing is disabled.
	Tracing instrument.TracingConfiguration `yaml:"tracing"`

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

	// Carbon is the carbon configuration.
	Carbon *CarbonConfiguration `yaml:"carbon"`

	// Limits specifies limits on per-query resource usage.
	Limits LimitsConfiguration `yaml:"limits"`

	// LookbackDuration determines the lookback duration for queries
	LookbackDuration *time.Duration `yaml:"lookbackDuration"`

	// Cache configurations.
	Cache CacheConfiguration `yaml:"cache"`

	// ResultOptions are the results options for query.
	ResultOptions ResultOptions `yaml:"resultOptions"`
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

// ResultOptions are the result options for query.
type ResultOptions struct {
	// KeepNans keeps NaNs before returning query results.
	// The default is false, which matches Prometheus
	KeepNans bool `yaml:"keepNans"`
}

// CacheConfiguration is the cache configurations.
type CacheConfiguration struct {
	// QueryConversion cache policy.
	QueryConversion *QueryConversionCacheConfiguration `yaml:"queryConversion"`
}

// QueryConversionCacheConfiguration is the query conversion cache configuration.
type QueryConversionCacheConfiguration struct {
	Size *int `yaml:"size"`
}

// QueryConversionCacheConfiguration returns the query conversion cache configuration
// or default if none is specified.
func (c CacheConfiguration) QueryConversionCacheConfiguration() QueryConversionCacheConfiguration {
	if c.QueryConversion == nil {
		return QueryConversionCacheConfiguration{}
	}

	return *c.QueryConversion
}

// SizeOrDefault returns the provided size or the default value is none is
// provided.
func (q *QueryConversionCacheConfiguration) SizeOrDefault() int {
	if q.Size == nil {
		return defaultQueryConversionCacheSize
	}

	return *q.Size
}

// Validate validates the QueryConversionCacheConfiguration settings.
func (q *QueryConversionCacheConfiguration) Validate() error {
	if q.Size != nil && *q.Size <= 0 {
		return fmt.Errorf("must provide a positive size for query conversion config, instead got: %d", *q.Size)
	}

	return nil
}

// LimitsConfiguration represents limitations on resource usage in the query instance. Limits are split between per-query
// and global limits.
type LimitsConfiguration struct {
	// deprecated: use PerQuery.MaxComputedDatapoints instead.
	DeprecatedMaxComputedDatapoints int64 `yaml:"maxComputedDatapoints"`

	// Global configures limits which apply across all queries running on this
	// instance.
	Global GlobalLimitsConfiguration `yaml:"global"`

	// PerQuery configures limits which apply to each query individually.
	PerQuery PerQueryLimitsConfiguration `yaml:"perQuery"`
}

// MaxComputedDatapoints is a getter providing backwards compatibility between
// LimitsConfiguration.DeprecatedMaxComputedDatapoints and
// LimitsConfiguration.PerQuery.PrivateMaxComputedDatapoints. See
// LimitsConfiguration.PerQuery.PrivateMaxComputedDatapoints for a comment on
// the semantics.
func (lc *LimitsConfiguration) MaxComputedDatapoints() int64 {
	if lc.PerQuery.PrivateMaxComputedDatapoints != 0 {
		return lc.PerQuery.PrivateMaxComputedDatapoints
	}

	return lc.DeprecatedMaxComputedDatapoints
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
	// PrivateMaxComputedDatapoints limits the number of datapoints that can be
	// returned by a query. It's determined purely
	// from the size of the time range and the step size (end - start / step).
	//
	// N.B.: the hacky "Private" prefix is to indicate that callers should use
	// LimitsConfiguration.MaxComputedDatapoints() instead of accessing
	// this field directly.
	PrivateMaxComputedDatapoints int64 `yaml:"maxComputedDatapoints"`

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
	Ingester ingestm3msg.Configuration `yaml:"ingester"`

	// M3Msg is the configuration for m3msg server.
	M3Msg m3msg.Configuration `yaml:"m3msg"`
}

// CarbonConfiguration is the configuration for the carbon server.
type CarbonConfiguration struct {
	Ingester *CarbonIngesterConfiguration `yaml:"ingester"`
}

// CarbonIngesterConfiguration is the configuration struct for carbon ingestion.
type CarbonIngesterConfiguration struct {
	Debug          bool                              `yaml:"debug"`
	ListenAddress  string                            `yaml:"listenAddress"`
	MaxConcurrency int                               `yaml:"maxConcurrency"`
	Rules          []CarbonIngesterRuleConfiguration `yaml:"rules"`
}

// LookbackDurationOrDefault validates the LookbackDuration
func (c Configuration) LookbackDurationOrDefault() (time.Duration, error) {
	if c.LookbackDuration == nil {
		return defaultLookbackDuration, nil
	}

	v := *c.LookbackDuration
	if v < 0 {
		return 0, errors.New("lookbackDuration must be > 0")
	}

	return v, nil
}

// ListenAddressOrDefault returns the specified carbon ingester listen address if provided, or the
// default value if not.
func (c *CarbonIngesterConfiguration) ListenAddressOrDefault() string {
	if c.ListenAddress != "" {
		return c.ListenAddress
	}

	return defaultCarbonIngesterListenAddress
}

// RulesOrDefault returns the specified carbon ingester rules if provided, or generates reasonable
// defaults using the provided aggregated namespaces if not.
func (c *CarbonIngesterConfiguration) RulesOrDefault(namespaces m3.ClusterNamespaces) []CarbonIngesterRuleConfiguration {
	if len(c.Rules) > 0 {
		return c.Rules
	}

	if namespaces.NumAggregatedClusterNamespaces() == 0 {
		return nil
	}

	// Default to fanning out writes for all metrics to all aggregated namespaces if any exists.
	policies := []CarbonIngesterStoragePolicyConfiguration{}
	for _, ns := range namespaces {
		if ns.Options().Attributes().MetricsType == storage.AggregatedMetricsType {
			policies = append(policies, CarbonIngesterStoragePolicyConfiguration{
				Resolution: ns.Options().Attributes().Resolution,
				Retention:  ns.Options().Attributes().Retention,
			})
		}
	}

	if len(policies) == 0 {
		return nil
	}

	// Create a single catch-all rule with a policy for each of the aggregated namespaces we
	// enumerated above.
	aggregationEnabled := true
	return []CarbonIngesterRuleConfiguration{
		{
			Pattern: graphite.MatchAllPattern,
			Aggregation: CarbonIngesterAggregationConfiguration{
				Enabled: &aggregationEnabled,
				Type:    &defaultCarbonIngesterAggregationType,
			},
			Policies: policies,
		},
	}
}

// CarbonIngesterRuleConfiguration is the configuration struct for a carbon
// ingestion rule.
type CarbonIngesterRuleConfiguration struct {
	Pattern     string                                     `yaml:"pattern"`
	Aggregation CarbonIngesterAggregationConfiguration     `yaml:"aggregation"`
	Policies    []CarbonIngesterStoragePolicyConfiguration `yaml:"policies"`
}

// CarbonIngesterAggregationConfiguration is the configuration struct
// for the aggregation for a carbon ingest rule's storage policy.
type CarbonIngesterAggregationConfiguration struct {
	Enabled *bool             `yaml:"enabled"`
	Type    *aggregation.Type `yaml:"type"`
}

// EnabledOrDefault returns whether aggregation should be enabled based on the provided configuration,
// or a default value otherwise.
func (c *CarbonIngesterAggregationConfiguration) EnabledOrDefault() bool {
	if c.Enabled != nil {
		return *c.Enabled
	}

	return true
}

// TypeOrDefault returns the aggregation type that should be used based on the provided configuration,
// or a default value otherwise.
func (c *CarbonIngesterAggregationConfiguration) TypeOrDefault() aggregation.Type {
	if c.Type != nil {
		return *c.Type
	}

	return defaultCarbonIngesterAggregationType
}

// CarbonIngesterStoragePolicyConfiguration is the configuration struct for
// a carbon rule's storage policies.
type CarbonIngesterStoragePolicyConfiguration struct {
	Resolution time.Duration `yaml:"resolution" validate:"nonzero"`
	Retention  time.Duration `yaml:"retention" validate:"nonzero"`
}

// LocalConfiguration is the local embedded configuration if running
// coordinator embedded in the DB.
type LocalConfiguration struct {
	// Namespaces is the list of namespaces that the local embedded DB has.
	Namespaces []m3.ClusterStaticNamespaceConfiguration `yaml:"namespaces"`
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
	// If not provided, defaults to `__name__`.
	MetricName string `yaml:"metricName"`

	// BucketName specifies the tag name that corresponds to the metric's bucket.
	// If not provided, defaults to `le`.
	BucketName string `yaml:"bucketName"`

	// Scheme determines the default ID generation scheme. Defaults to TypeLegacy.
	Scheme models.IDSchemeType `yaml:"idScheme"`
}

// TagOptionsFromConfig translates tag option configuration into tag options.
func TagOptionsFromConfig(cfg TagOptionsConfiguration) (models.TagOptions, error) {
	opts := models.NewTagOptions()
	name := cfg.MetricName
	if name != "" {
		opts = opts.SetMetricName([]byte(name))
	}

	bucket := cfg.BucketName
	if bucket != "" {
		opts = opts.SetBucketName([]byte(bucket))
	}

	if cfg.Scheme == models.TypeDefault {
		// If no config has been set, error.
		docLink := xdocs.Path("how_to/query#migration")
		return nil, fmt.Errorf(errNoIDGenerationScheme, docLink)
	}

	opts = opts.SetIDSchemeType(cfg.Scheme)
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return opts, nil
}
