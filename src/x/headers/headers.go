// Copyright (c) 2020 Uber Technologies, Inc.
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

package headers

const (
	// M3HeaderPrefix is the prefix all M3-specific headers that affect query or
	// write behavior (not necessarily m3admin headers) are guaranteed to have.
	M3HeaderPrefix = "M3-"

	// WarningsHeader is the M3 warnings header when to display a warning to a
	// user.
	WarningsHeader = M3HeaderPrefix + "Warnings"

	// RetryHeader is the M3 retry header to display when it is safe to retry.
	RetryHeader = M3HeaderPrefix + "Retry"

	// ServedByHeader is the M3 query storage execution breakdown.
	ServedByHeader = M3HeaderPrefix + "Storage-By"

	// DeprecatedHeader is the M3 deprecated header.
	DeprecatedHeader = M3HeaderPrefix + "Deprecated"

	// EngineHeaderName defines header name which is used to switch between
	// prometheus and m3query engines.
	EngineHeaderName = M3HeaderPrefix + "Engine"

	// MetricsTypeHeader sets the write or read metrics type to restrict
	// metrics to.
	// Valid values are "unaggregated" or "aggregated".
	MetricsTypeHeader = M3HeaderPrefix + "Metrics-Type"

	// PromTypeHeader sets the prometheus metric type. Valid values are
	// "counter", "gauge", etc. (see src/query/api/v1/handler/prometheus/remote/write.go
	// field `headerToMetricType`)
	PromTypeHeader = "Prometheus-Metric-Type"

	// WriteTypeHeader is a header that controls if default
	// writes should be written to both unaggregated and aggregated
	// namespaces, or if unaggregated values are skipped and
	// only aggregated values are written.
	// Valid values are "default" or "aggregate".
	WriteTypeHeader = M3HeaderPrefix + "Write-Type"

	// SourceHeader tracks bytes and docs read for the given source, if provided.
	SourceHeader = M3HeaderPrefix + "Source"

	// DefaultWriteType is the default write type.
	DefaultWriteType = "default"

	// AggregateWriteType is the aggregate write type. This writes to
	// only aggregated namespaces
	AggregateWriteType = "aggregate"

	// MetricsStoragePolicyHeader specifies the resolution and retention of
	// metrics being written or read.
	// In the form of a storage policy string, e.g. "1m:14d".
	// Only required if the metrics type header does not specify unaggregated
	// metrics type.
	MetricsStoragePolicyHeader = M3HeaderPrefix + "Storage-Policy"

	// MetricsRestrictByStoragePoliciesHeader provides the policies options to
	// enforce on queries, in the form of a list of storage policies.
	// "1m:14d;5m:60d"
	MetricsRestrictByStoragePoliciesHeader = M3HeaderPrefix + "Restrict-By-Storage-Policies"

	// RestrictByTagsJSONHeader provides tag options to enforces on queries,
	// in JSON format. See `handler.stringTagOptions` for definitions.`
	RestrictByTagsJSONHeader = M3HeaderPrefix + "Restrict-By-Tags-JSON"

	// MapTagsByJSONHeader provides the ability to mutate tags of timeseries in
	// incoming write requests. See `MapTagsOptions` for structure.
	MapTagsByJSONHeader = M3HeaderPrefix + "Map-Tags-JSON"

	// LimitMaxSeriesHeader is the M3 limit timeseries header that limits
	// the number of time series returned by each storage node.
	LimitMaxSeriesHeader = M3HeaderPrefix + "Limit-Max-Series"

	// LimitInstanceMultipleHeader overrides the PerQueryLimitsConfiguration.InstanceMultiple for the request.
	LimitInstanceMultipleHeader = M3HeaderPrefix + "Limit-Instance-Multiple"

	// LimitMaxDocsHeader is the M3 limit docs header that limits
	// the number of docs returned by each storage node.
	LimitMaxDocsHeader = M3HeaderPrefix + "Limit-Max-Docs"

	// LimitMaxRangeHeader is the M3 limit range header that limits
	// the time range returned by each storage node.
	LimitMaxRangeHeader = M3HeaderPrefix + "Limit-Max-Range"

	// LimitMaxReturnedDatapointsHeader is the M3 header that limits
	// the number of datapoints returned in total to the client.
	LimitMaxReturnedDatapointsHeader = M3HeaderPrefix + "Limit-Max-Returned-Datapoints"

	// LimitMaxReturnedSeriesHeader is the M3 header that limits
	// the number of series returned in total to the client.
	LimitMaxReturnedSeriesHeader = M3HeaderPrefix + "Limit-Max-Returned-Series"

	// LimitMaxReturnedSeriesMetadataHeader is the M3 header that limits
	// the number of series metadata returned in total to the client.
	LimitMaxReturnedSeriesMetadataHeader = M3HeaderPrefix + "Limit-Max-Returned-SeriesMetadata"

	// LimitRequireExhaustiveHeader is the M3 limit exhaustive header that will
	// ensure M3 returns an error if the results set is not exhaustive.
	LimitRequireExhaustiveHeader = M3HeaderPrefix + "Limit-Require-Exhaustive"

	// LimitRequireNoWaitHeader is the M3 header that ensures
	// M3 returns an error if query execution must wait for permits.
	LimitRequireNoWaitHeader = M3HeaderPrefix + "Limit-Require-No-Wait"

	// LimitMaxMetricMetadataStatsHeader is the M3 header that limits
	// the number of metric metadata stats returned in M3-Metric-Stats.
	LimitMaxMetricMetadataStatsHeader = M3HeaderPrefix + "Limit-Max-Metric-Metadata-Stats"

	// UnaggregatedStoragePolicy specifies the unaggregated storage policy.
	UnaggregatedStoragePolicy = "unaggregated"

	// DefaultServiceEnvironment is the default service ID environment.
	DefaultServiceEnvironment = "default_env"
	// DefaultServiceZone is the default service ID zone.
	DefaultServiceZone = "embedded"

	// HeaderClusterEnvironmentName is the header used to specify the
	// environment name.
	HeaderClusterEnvironmentName = "Cluster-Environment-Name"
	// HeaderClusterZoneName is the header used to specify the zone name.
	HeaderClusterZoneName = "Cluster-Zone-Name"
	// HeaderDryRun is the header used to specify whether this should be a dry
	// run.
	HeaderDryRun = "Dry-Run"
	// HeaderForce is the header used to specify whether this should be a forced
	// operation.
	HeaderForce = "Force"

	// LimitHeader is the header added when returned series are limited.
	LimitHeader = M3HeaderPrefix + "Results-Limited"

	// ReturnedDataLimitedHeader is the header added when returned
	// data are limited either by series or datapoints.
	ReturnedDataLimitedHeader = M3HeaderPrefix + "Returned-Data-Limited"

	// ReturnedMetadataLimitedHeader is the header added when returned
	// metadata is limited.
	ReturnedMetadataLimitedHeader = M3HeaderPrefix + "Returned-Metadata-Limited"

	// TimeoutHeader is the header added with the effective timeout.
	TimeoutHeader = M3HeaderPrefix + "Timeout"

	// LimitHeaderSeriesLimitApplied is the header applied when fetch results
	// are maxed.
	LimitHeaderSeriesLimitApplied = "max_fetch_series_limit_applied"

	// WaitedHeader is the header added when permits had to be waited for.
	WaitedHeader = M3HeaderPrefix + "Waited"

	// FetchedSeriesCount is the header added that tracks the total number of
	// series that were fetched by the query, before computation.
	FetchedSeriesCount = M3HeaderPrefix + "Series-Count"

	// FetchedSeriesNoSamplesCount is the header added that tracks the total number of
	// fetched series that were fetched by the query but had no samples.
	FetchedSeriesNoSamplesCount = M3HeaderPrefix + "Series-No-Samples-Count"

	// FetchedSeriesWithSamplesCount is the header added that tracks the total number of
	// fetched series that were fetched by the query and had non-zero samples.
	FetchedSeriesWithSamplesCount = M3HeaderPrefix + "Series-With-Samples-Count"

	// FetchedAggregatedSeriesCount is the header added that tracks the total number of
	// aggregated series that were fetched by the query, before computation.
	FetchedAggregatedSeriesCount = M3HeaderPrefix + "Aggregated-Series-Count"

	// FetchedUnaggregatedSeriesCount is the header added that tracks the total number of
	// unaggregated series that were fetched by the query, before computation.
	FetchedUnaggregatedSeriesCount = M3HeaderPrefix + "Unaggregated-Series-Count"

	// MetricStats is the header added that tracks the unique set of metric stats
	// all series fetched by this query, before computation, by metric name.
	MetricStats = M3HeaderPrefix + "Metric-Stats"

	// NamespacesHeader is the header added that tracks the unique set of namespaces
	// read by this query.
	NamespacesHeader = M3HeaderPrefix + "Namespaces"

	// FetchedResponsesHeader is the header added that tracks the number of M3DB responses
	// read by this query.
	FetchedResponsesHeader = M3HeaderPrefix + "Fetched-Responses"

	// FetchedBytesEstimateHeader is the header added that tracks the estimated number
	// of bytes returned by all fetch responses (counted by FetchedResponsesHeader).
	FetchedBytesEstimateHeader = M3HeaderPrefix + "Fetched-Bytes-Estimate"

	// FetchedMetadataCount is the header added that tracks the total amount of
	// metadata that was fetched by the query, before computation.
	FetchedMetadataCount = M3HeaderPrefix + "Metadata-Count"

	// RenderFormat is used to switch result format for query results rendering.
	RenderFormat = M3HeaderPrefix + "Render-Format"

	// JSONDisableDisallowUnknownFields is header if set to true that allows
	// for clients to send fields unknown by a HTTP/JSON endpoint and still
	// parse the request, this is helpful for sending a request with a new
	// schema to an older instance and still have it respond successfully
	// using the fields it knows about.
	JSONDisableDisallowUnknownFields = M3HeaderPrefix + "JSON-Disable-Disallow-Unknown-Fields"

	// CustomResponseMetricsType is a header that, if set, will override the `type` tag
	// on the request's response metrics.
	CustomResponseMetricsType = M3HeaderPrefix + "Custom-Response-Metrics-Type"

	// RelatedQueriesHeader is a header that, if set, will be used by clients to send a set of colon separated
	// start/end time pairs as unix timestamps (e.g. 1635160222:1635166222). Multiple
	// RelatedQueriesHeader headers may NOT be sent. When multiple values are required, they can be separated
	// by a semicolons (e.g. startTs:endTs;startTs:endTs).
	RelatedQueriesHeader = M3HeaderPrefix + "Related-Queries"
)
