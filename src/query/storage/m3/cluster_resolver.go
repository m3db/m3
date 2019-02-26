// Copyright (c) 2019 Uber Technologies, Inc.
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

package m3

import (
	"sort"
	"time"

	"github.com/m3db/m3/src/query/storage/m3/multiresults"

	"github.com/m3db/m3/src/query/storage"
)

type unaggregatedNamespaceType uint8

const (
	partiallySatisfiesRange unaggregatedNamespaceType = iota
	fullySatisfiesRange
	disabled
)

type unaggregatedNamespaceDetails struct {
	satisfies        unaggregatedNamespaceType
	clusterNamespace ClusterNamespace
	retention        time.Duration
}

// resolveUnaggregatedNamespaceForQuery determines if the unaggregated namespace
// should be used, and if so, determines if it fully satisfies the query range.
func resolveUnaggregatedNamespaceForQuery(
	now time.Time,
	start time.Time,
	unaggregated ClusterNamespace,
	opts *storage.FanoutOptions,
) unaggregatedNamespaceDetails {
	if opts.FanoutUnaggregated == storage.FanoutForceDisable {
		return unaggregatedNamespaceDetails{satisfies: disabled}
	}

	retention := unaggregated.Options().Attributes().Retention
	unaggregatedStart := now.Add(-1 * retention)
	if unaggregatedStart.Before(start) || unaggregatedStart.Equal(start) {
		return unaggregatedNamespaceDetails{
			clusterNamespace: unaggregated,
			retention:        retention,
			satisfies:        fullySatisfiesRange,
		}
	}

	return unaggregatedNamespaceDetails{
		clusterNamespace: unaggregated,
		retention:        retention,
		satisfies:        partiallySatisfiesRange,
	}
}

type reusedAggregatedNamespaceSlices struct {
	completeAggregated []ClusterNamespace
	partialAggregated  []ClusterNamespace
}

// resolveClusterNamespacesForQuery returns the namespaces that need to be
// fanned out to depending on the query time and the namespaces configured.
func resolveClusterNamespacesForQuery(
	now time.Time,
	clusters Clusters,
	start time.Time,
	end time.Time,
	opts *storage.FanoutOptions,
) (multiresults.QueryFanoutType, ClusterNamespaces, error) {
	unaggregated := clusters.UnaggregatedClusterNamespace()
	unaggregatedRetention := unaggregated.Options().Attributes().Retention
	unaggregatedStart := now.Add(-1 * unaggregatedRetention)
	if unaggregatedStart.Before(start) || unaggregatedStart.Equal(start) {
		// Highest resolution is unaggregated, return if it can fulfill it
		return multiresults.NamespaceCoversAllQueryRange, ClusterNamespaces{unaggregated}, nil
	}

	// First determine if any aggregated clusters span the whole query range, if
	// so that's the most optimal strategy, choose the most granular resolution
	// that can and fan out to any partial aggregated namespaces that may holder
	// even more granular resolutions
	var r reusedAggregatedNamespaceSlices
	r = aggregatedNamespaces(clusters.ClusterNamespaces(), r,
		func(namespace ClusterNamespace) bool {
			// Include only if can fulfill the entire time range of the query
			clusterStart := now.Add(-1 * namespace.Options().Attributes().Retention)
			return clusterStart.Before(start) || clusterStart.Equal(start)
		}, opts)

	if len(r.completeAggregated) > 0 {
		// Return the most granular completed aggregated namespace and
		// any potentially more granular partial aggregated namespaces
		sort.Stable(ClusterNamespacesByResolutionAsc(r.completeAggregated))

		// Take most granular complete aggregated namespace
		result := r.completeAggregated[:1]
		completedAttrs := result[0].Options().Attributes()

		// Take any finer grain partially aggregated namespaces that
		// may contain a matching metric
		for _, n := range r.partialAggregated {
			if n.Options().Attributes().Resolution >= completedAttrs.Resolution {
				// Not more granular
				continue
			}
			result = append(result, n)
		}

		return multiresults.NamespaceCoversAllQueryRange, result, nil
	}

	// No complete aggregated namespaces can definitely fulfill the query,
	// so take the longest retention completed aggregated namespace to return
	// as much data as possible, along with any partially aggregated namespaces
	// that have either same retention and lower resolution or longer retention
	// than the complete aggregated namespace
	r = aggregatedNamespaces(clusters.ClusterNamespaces(), r, nil, opts)

	if len(r.completeAggregated) == 0 {
		// Absolutely no complete aggregated namespaces, need to fanout to all
		// partial aggregated namespaces as well as the unaggregated cluster
		// as we have no idea who has the longest retention
		result := append(r.partialAggregated, unaggregated)
		return multiresults.NamespaceCoversPartialQueryRange, result, nil
	}

	// Return the longest retention aggregated namespace and
	// any potentially more granular or longer retention partial
	// aggregated namespaces
	sort.Stable(sort.Reverse(ClusterNamespacesByRetentionAsc(r.completeAggregated)))

	// Take longest retention complete aggregated namespace or the unaggregated
	// cluster if that is longer than the longest aggregated namespace
	result := r.completeAggregated[:1]
	completedAttrs := result[0].Options().Attributes()
	if completedAttrs.Retention <= unaggregatedRetention {
		// If the longest aggregated cluster for some reason has lower retention
		// than the unaggregated cluster then we prefer the unaggregated cluster
		// as it has a complete data set and is always the most granular
		result[0] = unaggregated
		completedAttrs = unaggregated.Options().Attributes()
	}

	// Take any partially aggregated namespaces with longer retention or
	// same retention with more granular resolution that may contain
	// a matching metric
	for _, n := range r.partialAggregated {
		if n.Options().Attributes().Retention > completedAttrs.Retention {
			// Higher retention
			result = append(result, n)
			continue
		}
		if n.Options().Attributes().Retention == completedAttrs.Retention &&
			n.Options().Attributes().Resolution < completedAttrs.Resolution {
			// Same retention but more granular resolution
			result = append(result, n)
			continue
		}
	}

	return multiresults.NamespaceCoversPartialQueryRange, result, nil
}

// aggregatedNamespaces filters out clusters that do not meet the filter
// condition, and organizes remaining clusters in two lists if possible.
//
// NB: If fanout aggregation is disabled, no clusters will be returned as either
// partial or complete candidates. If fanout aggregation is forced to enabled
// then no filter is applied, and all namespaces are considered viable. In this
// case, the filter is used to determine if returned namespaces have the
// complete set of metrics.
//
// NB: If fanout optimization is enabled, add any aggregated namespaces that
// have a complete set of metrics to the completeAggregated slice list. If this
// optimization is disabled, or if none of the aggregated namespaces are
// guaranteed to have a complete set of all metrics, they are added to the
// partialAggregated list.
func aggregatedNamespaces(
	all ClusterNamespaces,
	slices reusedAggregatedNamespaceSlices,
	filter func(ClusterNamespace) bool,
	opts *storage.FanoutOptions,
) reusedAggregatedNamespaceSlices {
	// Reset reused slices as necessary
	if slices.completeAggregated == nil {
		slices.completeAggregated = make([]ClusterNamespace, 0, len(all))
	}
	slices.completeAggregated = slices.completeAggregated[:0]
	if slices.partialAggregated == nil {
		slices.partialAggregated = make([]ClusterNamespace, 0, len(all))
	}
	slices.partialAggregated = slices.partialAggregated[:0]

	for _, namespace := range all {
		opts := namespace.Options()
		if opts.Attributes().MetricsType != storage.AggregatedMetricsType {
			// Not an aggregated cluster
			continue
		}

		if filter != nil && !filter(namespace) {
			continue
		}

		downsampleOpts, err := opts.DownsampleOptions()
		if err != nil || !downsampleOpts.All {
			// Cluster does not contain all data, include as part of fan out
			// but separate from
			slices.partialAggregated = append(slices.partialAggregated, namespace)
			continue
		}

		slices.completeAggregated = append(slices.completeAggregated, namespace)
	}

	return slices
}
