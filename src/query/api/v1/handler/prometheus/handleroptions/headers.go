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

package handleroptions

import (
	"net/http"
	"strings"

	"github.com/m3db/m3/src/query/block"
)

const (
	// M3HeaderPrefix is the prefix all M3-specific headers that affect query or
	// write behavior (not necessarily m3admin headers) are guaranteed to have.
	M3HeaderPrefix = "M3-"

	// WarningsHeader is the M3 warnings header when to display a warning to a user.
	WarningsHeader = M3HeaderPrefix + "Warnings"

	// RetryHeader is the M3 retry header to display when it is safe to retry.
	RetryHeader = M3HeaderPrefix + "Retry"

	// ServedByHeader is the M3 query storage execution breakdown.
	ServedByHeader = M3HeaderPrefix + "Storage-By"

	// DeprecatedHeader is the M3 deprecated header.
	DeprecatedHeader = M3HeaderPrefix + "Deprecated"

	// MetricsTypeHeader sets the write or read metrics type to restrict
	// metrics to.
	// Valid values are "unaggregated" or "aggregated".
	MetricsTypeHeader = M3HeaderPrefix + "Metrics-Type"

	// WriteTypeHeader is a header that controls if default
	// writes should be written to both unaggregated and aggregated
	// namespaces, or if unaggregated values are skipped and
	// only aggregated values are written.
	// Valid values are "default" or "aggregate".
	WriteTypeHeader = M3HeaderPrefix + "Write-Type"

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

	// RestrictByTagsJSONHeader provides tag options to enforces on queries,
	// in JSON format. See `handler.stringTagOptions` for definitions.`
	RestrictByTagsJSONHeader = M3HeaderPrefix + "Restrict-By-Tags-JSON"

	// MapTagsByJSONHeader provides the ability to mutate tags of timeseries in
	// incoming write requests. See `MapTagsOptions` for structure.
	MapTagsByJSONHeader = M3HeaderPrefix + "Map-Tags-JSON"

	// LimitMaxSeriesHeader is the M3 limit timeseries header that limits
	// the number of time series returned by each storage node.
	LimitMaxSeriesHeader = M3HeaderPrefix + "Limit-Max-Series"

	// LimitMaxDocsHeader is the M3 limit docs header that limits
	// the number of docs returned by each storage node.
	LimitMaxDocsHeader = M3HeaderPrefix + "Limit-Max-Docs"

	// LimitRequireExhaustiveHeader is the M3 limit exhaustive header that will
	// ensure M3 returns an error if the results set is not exhaustive.
	LimitRequireExhaustiveHeader = M3HeaderPrefix + "Limit-Require-Exhaustive"

	// UnaggregatedStoragePolicy specifies the unaggregated storage policy.
	UnaggregatedStoragePolicy = "unaggregated"

	// DefaultServiceEnvironment is the default service ID environment.
	DefaultServiceEnvironment = "default_env"
	// DefaultServiceZone is the default service ID zone.
	DefaultServiceZone = "embedded"

	// HeaderClusterEnvironmentName is the header used to specify the environment
	// name.
	HeaderClusterEnvironmentName = "Cluster-Environment-Name"
	// HeaderClusterZoneName is the header used to specify the zone name.
	HeaderClusterZoneName = "Cluster-Zone-Name"
	// HeaderDryRun is the header used to specify whether this should be a dry
	// run.
	HeaderDryRun = "Dry-Run"
	// HeaderForce is the header used to specify whether this should be a forced operation.
	HeaderForce = "Force"

	// LimitHeader is the header added when returned series are limited.
	LimitHeader = M3HeaderPrefix + "Results-Limited"

	// LimitHeaderSeriesLimitApplied is the header applied when fetch results are
	// maxed.
	LimitHeaderSeriesLimitApplied = "max_fetch_series_limit_applied"
)

// AddWarningHeaders adds any warning headers present in the result's metadata.
// No-op if no warnings encountered.
func AddWarningHeaders(w http.ResponseWriter, meta block.ResultMetadata) {
	ex := meta.Exhaustive
	warns := len(meta.Warnings)
	if !ex {
		warns++
	}

	if warns == 0 {
		return
	}

	warnings := make([]string, 0, warns)
	if !ex {
		warnings = append(warnings, LimitHeaderSeriesLimitApplied)
	}

	for _, warn := range meta.Warnings {
		warnings = append(warnings, warn.Header())
	}

	w.Header().Set(LimitHeader, strings.Join(warnings, ","))
}
