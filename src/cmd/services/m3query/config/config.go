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
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/config/listenaddress"
	"github.com/m3db/m3/src/x/cost"
	xdocs "github.com/m3db/m3/src/x/docs"
	"github.com/m3db/m3/src/x/instrument"
	xlog "github.com/m3db/m3/src/x/log"
	"github.com/m3db/m3/src/x/opentracing"
)

// BackendStorageType is an enum for different backends.
type BackendStorageType string

const (
	// GRPCStorageType is for backends which only support grpc endpoints.
	GRPCStorageType BackendStorageType = "grpc"
	// M3DBStorageType is for m3db backend.
	M3DBStorageType BackendStorageType = "m3db"
	// NoopEtcdStorageType is for a noop backend which returns empty results for
	// any query and blackholes any writes, but requires that a valid etcd cluster
	// is defined and can be connected to. Primarily used for standalone
	// coordinators used only to serve m3admin APIs.
	NoopEtcdStorageType BackendStorageType = "noop-etcd"

	defaultCarbonIngesterListenAddress = "0.0.0.0:7204"
	errNoIDGenerationScheme            = "error: a recent breaking change means that an ID " +
		"generation scheme is required in coordinator configuration settings. " +
		"More information is available here: %s"

	defaultQueryTimeout = 30 * time.Second

	defaultPrometheusMaxSamplesPerQuery = 100000000
)

var (
	// 5m is the default lookback in Prometheus
	defaultLookbackDuration = 5 * time.Minute

	defaultCarbonIngesterAggregationType = aggregation.Mean

	defaultStorageQuerySeriesLimit = 10000
	defaultStorageQueryDocsLimit   = 0 // Default OFF.
)

// Configuration is the configuration for the query service.
type Configuration struct {
	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// Logging configuration.
	Logging xlog.Configuration `yaml:"logging"`

	// Tracing configures opentracing. If not provided, tracing is disabled.
	Tracing opentracing.TracingConfiguration `yaml:"tracing"`

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

	// WriteForwarding is the write forwarding options.
	WriteForwarding WriteForwardingConfiguration `yaml:"writeForwarding"`

	// Downsample configurates how the metrics should be downsampled.
	Downsample downsample.Configuration `yaml:"downsample"`

	// Ingest is the ingest server.
	Ingest *IngestConfiguration `yaml:"ingest"`

	// Carbon is the carbon configuration.
	Carbon *CarbonConfiguration `yaml:"carbon"`

	// Query is the query configuration.
	Query QueryConfiguration `yaml:"query"`

	// Limits specifies limits on per-query resource usage.
	Limits LimitsConfiguration `yaml:"limits"`

	// LookbackDuration determines the lookback duration for queries
	LookbackDuration *time.Duration `yaml:"lookbackDuration"`

	// ResultOptions are the results options for query.
	ResultOptions ResultOptions `yaml:"resultOptions"`

	// Experimental is the configuration for the experimental API group.
	Experimental ExperimentalAPIConfiguration `yaml:"experimental"`

	// Cache configurations.
	//
	// Deprecated: cache configurations are no longer supported. Remove from file
	// when we can make breaking changes.
	// (If/when removed it will make existing configurations with the cache
	// stanza not able to startup the binary since we parse YAML in strict mode
	// by default).
	DeprecatedCache CacheConfiguration `yaml:"cache"`

	// MultiProcess is the multi-process configuration.
	MultiProcess MultiProcessConfiguration `yaml:"multiProcess"`
}

// WriteForwardingConfiguration is the write forwarding configuration.
type WriteForwardingConfiguration struct {
	PromRemoteWrite handleroptions.PromWriteHandlerForwardingOptions `yaml:"promRemoteWrite"`
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

// CacheConfiguration contains the cache configurations.
type CacheConfiguration struct {
	// Deprecated: remove from config.
	DeprecatedQueryConversion *DeprecatedQueryConversionCacheConfiguration `yaml:"queryConversion"`
}

// DeprecatedQueryConversionCacheConfiguration is deprecated: remove from config.
type DeprecatedQueryConversionCacheConfiguration struct {
	Size *int `yaml:"size"`
}

// ResultOptions are the result options for query.
type ResultOptions struct {
	// KeepNans keeps NaNs before returning query results.
	// The default is false, which matches Prometheus
	KeepNans bool `yaml:"keepNans"`
}

// QueryConfiguration is the query configuration.
type QueryConfiguration struct {
	// Timeout is the query timeout.
	Timeout *time.Duration `yaml:"timeout"`
	// DefaultEngine is the default query engine.
	DefaultEngine string `yaml:"defaultEngine"`
	// ConsolidationConfiguration are configs for consolidating fetched queries.
	ConsolidationConfiguration ConsolidationConfiguration `yaml:"consolidation"`
	// Prometheus is prometheus client configuration.
	Prometheus PrometheusQueryConfiguration `yaml:"prometheus"`
	// RestrictTags is an optional configuration that can be set to restrict
	// all queries with certain tags by.
	RestrictTags *RestrictTagsConfiguration `yaml:"restrictTags"`
}

// TimeoutOrDefault returns the configured timeout or default value.
func (c QueryConfiguration) TimeoutOrDefault() time.Duration {
	if v := c.Timeout; v != nil {
		return *v
	}
	return defaultQueryTimeout
}

// RestrictTagsAsStorageRestrictByTag returns restrict tags as
// storage options to restrict all queries by default.
func (c QueryConfiguration) RestrictTagsAsStorageRestrictByTag() (*storage.RestrictByTag, bool, error) {
	if c.RestrictTags == nil {
		return nil, false, nil
	}

	var (
		cfg    = *c.RestrictTags
		result = handleroptions.StringTagOptions{
			Restrict: make([]handleroptions.StringMatch, 0, len(cfg.Restrict)),
			Strip:    cfg.Strip,
		}
	)
	for _, elem := range cfg.Restrict {
		value := handleroptions.StringMatch(elem)
		result.Restrict = append(result.Restrict, value)
	}

	opts, err := result.StorageOptions()
	if err != nil {
		return nil, false, err
	}

	return opts, true, nil
}

// RestrictTagsConfiguration applies tag restriction to all queries.
type RestrictTagsConfiguration struct {
	Restrict []StringMatch `yaml:"match"`
	Strip    []string      `yaml:"strip"`
}

// StringMatch is an easy to use representation of models.Matcher.
type StringMatch struct {
	Name  string `yaml:"name"`
	Type  string `yaml:"type"`
	Value string `yaml:"value"`
}

// ConsolidationConfiguration are configs for consolidating fetched queries.
type ConsolidationConfiguration struct {
	// MatchType determines the options by which series should match.
	MatchType consolidators.MatchType `yaml:"matchType"`
}

// PrometheusQueryConfiguration is the prometheus query engine configuration.
type PrometheusQueryConfiguration struct {
	// MaxSamplesPerQuery is the limit on fetched samples per query.
	MaxSamplesPerQuery *int `yaml:"maxSamplesPerQuery"`
}

// MaxSamplesPerQueryOrDefault returns the max samples per query or default.
func (c PrometheusQueryConfiguration) MaxSamplesPerQueryOrDefault() int {
	if v := c.MaxSamplesPerQuery; v != nil {
		return *v
	}

	return defaultPrometheusMaxSamplesPerQuery
}

// LimitsConfiguration represents limitations on resource usage in the query
// instance. Limits are split between per-query and global limits.
type LimitsConfiguration struct {
	// PerQuery configures limits which apply to each query individually.
	PerQuery PerQueryLimitsConfiguration `yaml:"perQuery"`

	// Global configures limits which apply across all queries running on this
	// instance.
	Global GlobalLimitsConfiguration `yaml:"global"`

	// deprecated: use PerQuery.MaxComputedDatapoints instead.
	DeprecatedMaxComputedDatapoints int `yaml:"maxComputedDatapoints"`
}

// MaxComputedDatapoints is a getter providing backwards compatibility between
// LimitsConfiguration.DeprecatedMaxComputedDatapoints and
// LimitsConfiguration.PerQuery.PrivateMaxComputedDatapoints. See
// LimitsConfiguration.PerQuery.PrivateMaxComputedDatapoints for a comment on
// the semantics.
func (lc LimitsConfiguration) MaxComputedDatapoints() int {
	if lc.PerQuery.PrivateMaxComputedDatapoints != 0 {
		return lc.PerQuery.PrivateMaxComputedDatapoints
	}

	return lc.DeprecatedMaxComputedDatapoints
}

// GlobalLimitsConfiguration represents limits on resource usage across a query
// instance. Zero or negative values imply no limit.
type GlobalLimitsConfiguration struct {
	// MaxFetchedDatapoints limits the max number of datapoints allowed to be
	// used by all queries at any point in time, this is applied at the query
	// service after the result has been returned by a storage node.
	MaxFetchedDatapoints int `yaml:"maxFetchedDatapoints"`
}

// AsLimitManagerOptions converts this configuration to
// cost.LimitManagerOptions for MaxFetchedDatapoints.
func (l *GlobalLimitsConfiguration) AsLimitManagerOptions() cost.LimitManagerOptions {
	return toLimitManagerOptions(l.MaxFetchedDatapoints)
}

// PerQueryLimitsConfiguration represents limits on resource usage within a
// single query. Zero or negative values imply no limit.
type PerQueryLimitsConfiguration struct {
	// MaxFetchedSeries limits the number of time series returned for any given
	// individual storage node per query, before returning result to query
	// service.
	MaxFetchedSeries int `yaml:"maxFetchedSeries"`

	// MaxFetchedDocs limits the number of index documents matched for any given
	// individual storage node per query, before returning result to query
	// service.
	MaxFetchedDocs int `yaml:"maxFetchedDocs"`

	// RequireExhaustive results in an error if the query exceeds any limit.
	RequireExhaustive bool `yaml:"requireExhaustive"`

	// MaxFetchedDatapoints limits the max number of datapoints allowed to be
	// used by a given query, this is applied at the query service after the
	// result has been returned by a storage node.
	MaxFetchedDatapoints int `yaml:"maxFetchedDatapoints"`

	// PrivateMaxComputedDatapoints limits the number of datapoints that can be
	// returned by a query. It's determined purely
	// from the size of the time range and the step size (end - start / step).
	//
	// N.B.: the hacky "Private" prefix is to indicate that callers should use
	// LimitsConfiguration.MaxComputedDatapoints() instead of accessing
	// this field directly.
	PrivateMaxComputedDatapoints int `yaml:"maxComputedDatapoints"`
}

// AsLimitManagerOptions converts this configuration to
// cost.LimitManagerOptions for MaxFetchedDatapoints.
func (l *PerQueryLimitsConfiguration) AsLimitManagerOptions() cost.LimitManagerOptions {
	return toLimitManagerOptions(l.MaxFetchedDatapoints)
}

// AsFetchOptionsBuilderLimitsOptions converts this configuration to
// handleroptions.FetchOptionsBuilderLimitsOptions.
func (l *PerQueryLimitsConfiguration) AsFetchOptionsBuilderLimitsOptions() handleroptions.FetchOptionsBuilderLimitsOptions {
	seriesLimit := defaultStorageQuerySeriesLimit
	if v := l.MaxFetchedSeries; v > 0 {
		seriesLimit = v
	}

	docsLimit := defaultStorageQueryDocsLimit
	if v := l.MaxFetchedDocs; v > 0 {
		docsLimit = v
	}

	return handleroptions.FetchOptionsBuilderLimitsOptions{
		SeriesLimit:       int(seriesLimit),
		DocsLimit:         int(docsLimit),
		RequireExhaustive: l.RequireExhaustive,
	}
}

func toLimitManagerOptions(limit int) cost.LimitManagerOptions {
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
	// Deprecated: simply use the logger debug level, this has been deprecated
	// in favor of setting the log level to debug.
	DeprecatedDebug bool                              `yaml:"debug"`
	ListenAddress   string                            `yaml:"listenAddress"`
	MaxConcurrency  int                               `yaml:"maxConcurrency"`
	Rules           []CarbonIngesterRuleConfiguration `yaml:"rules"`
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
		if ns.Options().Attributes().MetricsType == storagemetadata.AggregatedMetricsType {
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
	Continue    bool                                       `yaml:"continue"`
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

// RemoteConfigurations is a set of remote host configurations.
type RemoteConfigurations []RemoteConfiguration

// RemoteConfiguration is the configuration for a single remote host.
type RemoteConfiguration struct {
	// Name is the name for the remote zone.
	Name string `yaml:"name"`
	// RemoteListenAddresses is the remote listen addresses to call for remote
	// coordinator calls in the remote zone.
	RemoteListenAddresses []string `yaml:"remoteListenAddresses"`
	// ErrorBehavior overrides the default error behavior for this host.
	//
	// NB: defaults to warning on error.
	ErrorBehavior *storage.ErrorBehavior `yaml:"errorBehavior"`
}

// RPCConfiguration is the RPC configuration for the coordinator for
// the GRPC server used for remote coordinator to coordinator calls.
type RPCConfiguration struct {
	// Enabled determines if coordinator RPC is enabled for remote calls.
	//
	// NB: this is no longer necessary to set to true if RPC is desired; enabled
	// status is inferred based on which other options are provided;
	// this remains for back-compat, and for disabling any existing RPC options.
	Enabled *bool `yaml:"enabled"`

	// ListenAddress is the RPC server listen address.
	ListenAddress string `yaml:"listenAddress"`

	// Remotes are the configuration settings for remote coordinator zones.
	Remotes RemoteConfigurations `yaml:"remotes"`

	// RemoteListenAddresses is the remote listen addresses to call for
	// remote coordinator calls.
	//
	// NB: this is deprecated in favor of using RemoteZones, as setting
	// RemoteListenAddresses will only allow for a single remote zone to be used.
	RemoteListenAddresses []string `yaml:"remoteListenAddresses"`

	// ErrorBehavior overrides the default error behavior for all rpc hosts.
	//
	// NB: defaults to warning on error.
	ErrorBehavior *storage.ErrorBehavior `yaml:"errorBehavior"`

	// ReflectionEnabled will enable reflection on the GRPC server, useful
	// for testing connectivity with grpcurl, etc.
	ReflectionEnabled bool `yaml:"reflectionEnabled"`
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

	// Filters are optional tag filters, removing all series with tags
	// matching the filter from computations.
	Filters []TagFilter `yaml:"filters"`
}

// TagFilter is a tag filter.
type TagFilter struct {
	// Values are the values to filter.
	//
	// NB:If this is unset, all series containing
	// a tag with given `Name` are filtered.
	Values []string `yaml:"values"`
	// Name is the tag name.
	Name string `yaml:"name"`
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

	if cfg.Filters != nil {
		filters := make([]models.Filter, 0, len(cfg.Filters))
		for _, filter := range cfg.Filters {
			values := make([][]byte, 0, len(filter.Values))
			for _, strVal := range filter.Values {
				values = append(values, []byte(strVal))
			}

			filters = append(filters, models.Filter{
				Name:   []byte(filter.Name),
				Values: values,
			})
		}

		opts = opts.SetFilters(filters)
	}

	return opts, nil
}

// ExperimentalAPIConfiguration is the configuration for the experimental API group.
type ExperimentalAPIConfiguration struct {
	Enabled bool `yaml:"enabled"`
}

// MultiProcessConfiguration is the multi-process configuration which
// allows running multiple sub-processes of an instance reusing the
// same listen ports.
type MultiProcessConfiguration struct {
	// Enabled is whether to enable multi-process execution.
	Enabled bool `yaml:"enabled"`
	// Count is the number of sub-processes to run, leave zero
	// to auto-detect based on number of CPUs.
	Count int `yaml:"count" validate:"min=0"`
	// PerCPU is the factor of processes to run per CPU, leave
	// zero to use the default of 0.5 per CPU (i.e. one process for
	// every two CPUs).
	PerCPU float64 `yaml:"perCPU" validate:"min=0.0, max=0.0"`
	// GoMaxProcs if set will explicitly set the child GOMAXPROCs env var.
	GoMaxProcs int `yaml:"goMaxProcs"`
}
