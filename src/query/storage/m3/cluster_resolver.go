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
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
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
	now, start time.Time,
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

// resolveClusterNamespacesForQuery returns the namespaces that need to be
// fanned out to depending on the query time and the namespaces configured.
func resolveClusterNamespacesForQuery(
	now, start, end time.Time,
	clusters Clusters,
	opts *storage.FanoutOptions,
	restrict *storage.RestrictQueryOptions,
) (consolidators.QueryFanoutType, ClusterNamespaces, error) {
	if typeRestrict := restrict.GetRestrictByType(); typeRestrict != nil {
		// If a specific restriction is set, then attempt to satisfy.
		return resolveClusterNamespacesForQueryWithRestrictQueryOptions(now,
			start, clusters, *typeRestrict)
	}

	// First check if the unaggregated cluster can fully satisfy the query range.
	// If so, return it and shortcircuit, as unaggregated will necessarily have
	// every metric.
	ns, initialized := clusters.UnaggregatedClusterNamespace()
	if !initialized {
		return consolidators.NamespaceInvalid, nil, errUnaggregatedNamespaceUninitialized
	}

	unaggregated := resolveUnaggregatedNamespaceForQuery(now, start, ns, opts)
	if unaggregated.satisfies == fullySatisfiesRange {
		return consolidators.NamespaceCoversAllQueryRange,
			ClusterNamespaces{unaggregated.clusterNamespace},
			nil
	}

	if opts.FanoutAggregated == storage.FanoutForceDisable {
		if unaggregated.satisfies == partiallySatisfiesRange {
			return consolidators.NamespaceCoversPartialQueryRange,
				ClusterNamespaces{unaggregated.clusterNamespace}, nil
		}

		return consolidators.NamespaceInvalid, nil, errUnaggregatedAndAggregatedDisabled
	}

	// The filter function will drop namespaces which do not cover the entire
	// query range from contention.
	//
	// NB: if fanout aggregation is forced on, the filter instead forces clusters
	// that do not cover the range to be set as partially aggregated.
	coversRangeFilter := newCoversRangeFilter(coversRangeFilterOptions{
		now:        now,
		queryStart: start,
	})

	// Filter aggregated namespaces by filter function and options.
	var r reusedAggregatedNamespaceSlices
	r = aggregatedNamespaces(clusters.ClusterNamespaces(), r, coversRangeFilter, opts)

	// If any of the aggregated clusters have a complete set of metrics, use
	// those that have the smallest resolutions, supplemented by lower resolution
	// partially aggregated metrics.
	if len(r.completeAggregated) > 0 {
		sort.Stable(ClusterNamespacesByResolutionAsc(r.completeAggregated))
		// Take most granular complete aggregated namespace.
		result := r.completeAggregated[:1]
		completedAttrs := result[0].Options().Attributes()
		// Also include any finer grain partially aggregated namespaces that
		// may contain a matching metric.
		for _, n := range r.partialAggregated {
			if n.Options().Attributes().Resolution < completedAttrs.Resolution {
				// More granular resolution.
				result = append(result, n)
			}
		}

		return consolidators.NamespaceCoversAllQueryRange, result, nil
	}

	// No complete aggregated namespaces can definitely fulfill the query,
	// so take the longest retention completed aggregated namespace to return
	// as much data as possible, along with any partially aggregated namespaces
	// that have either same retention and lower resolution or longer retention
	// than the complete aggregated namespace.
	r = aggregatedNamespaces(clusters.ClusterNamespaces(), r, nil, opts)
	if len(r.completeAggregated) == 0 {
		// Absolutely no complete aggregated namespaces, need to fanout to all
		// partial aggregated namespaces as well as the unaggregated cluster
		// as we have no idea which has the longest retention.
		result := r.partialAggregated
		// If unaggregated namespace can partially satisfy this range, add it as a
		// fanout contender.
		if unaggregated.satisfies == partiallySatisfiesRange {
			result = append(result, unaggregated.clusterNamespace)
		}

		// If any namespace currently in contention does not cover the entire query
		// range, set query fanout type to namespaceCoversPartialQueryRange.
		for _, n := range result {
			if !coversRangeFilter(n) {
				return consolidators.NamespaceCoversPartialQueryRange, result, nil
			}
		}

		// Otherwise, all namespaces cover the query range.
		return consolidators.NamespaceCoversAllQueryRange, result, nil
	}

	// Return the longest retention aggregated namespace and
	// any potentially more granular or longer retention partial
	// aggregated namespaces.
	sort.Stable(sort.Reverse(ClusterNamespacesByRetentionAsc(r.completeAggregated)))

	// Take longest retention complete aggregated namespace or the unaggregated
	// cluster if that is longer than the longest aggregated namespace.
	result := r.completeAggregated[:1]
	completedAttrs := result[0].Options().Attributes()
	if unaggregated.satisfies == partiallySatisfiesRange {
		if completedAttrs.Retention <= unaggregated.retention {
			// If the longest aggregated cluster for some reason has lower retention
			// than the unaggregated cluster then we prefer the unaggregated cluster
			// as it has a complete data set and is always the most granular.
			result[0] = unaggregated.clusterNamespace
			completedAttrs = unaggregated.clusterNamespace.Options().Attributes()
		}
	}

	// Take any partially aggregated namespaces with longer retention or
	// same retention with more granular resolution that may contain
	// a matching metric.
	for _, n := range r.partialAggregated {
		if n.Options().Attributes().Retention > completedAttrs.Retention {
			// Higher retention.
			result = append(result, n)
		} else if n.Options().Attributes().Retention == completedAttrs.Retention &&
			n.Options().Attributes().Resolution < completedAttrs.Resolution {
			// Same retention but more granular resolution.
			result = append(result, n)
		}
	}

	return consolidators.NamespaceCoversPartialQueryRange, result, nil
}

type reusedAggregatedNamespaceSlices struct {
	completeAggregated []ClusterNamespace
	partialAggregated  []ClusterNamespace
}

func (slices reusedAggregatedNamespaceSlices) reset(
	size int,
) reusedAggregatedNamespaceSlices {
	// Initialize arrays if yet uninitialized.
	if slices.completeAggregated == nil {
		slices.completeAggregated = make([]ClusterNamespace, 0, size)
	} else {
		slices.completeAggregated = slices.completeAggregated[:0]
	}

	if slices.partialAggregated == nil {
		slices.partialAggregated = make([]ClusterNamespace, 0, size)
	} else {
		slices.partialAggregated = slices.partialAggregated[:0]
	}

	return slices
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
	// Reset reused slices.
	slices = slices.reset(len(all))

	// Otherwise the default and force enable is to fanout and treat
	// the aggregated namespaces differently (depending on whether they
	// have all the data).
	for _, namespace := range all {
		nsOpts := namespace.Options()
		if nsOpts.Attributes().MetricsType != storagemetadata.AggregatedMetricsType {
			// Not an aggregated cluster.
			continue
		}

		if filter != nil && !filter(namespace) {
			// Fails to satisfy filter.
			continue
		}

		// If not optimizing fanout to aggregated namespaces, set all aggregated
		// namespaces satisfying the filter as partially aggregated, as all metrics
		// do not necessarily appear in all namespaces, depending on configuration.
		if opts.FanoutAggregatedOptimized == storage.FanoutForceDisable {
			slices.partialAggregated = append(slices.partialAggregated, namespace)
			continue
		}

		// Otherwise, check downsample options for the namespace and determine if
		// this namespace is set as containing all metrics.
		downsampleOpts, err := nsOpts.DownsampleOptions()
		if err != nil {
			continue
		}

		if downsampleOpts.All {
			// This namespace has a complete set of metrics. Ensure that it passes
			// the filter if it was a forced addition, otherwise it may be too short
			// to cover the entire range and should be considered a partial result.
			slices.completeAggregated = append(slices.completeAggregated, namespace)
			continue
		}

		// This namespace does not necessarily have a complete set of metrics.
		slices.partialAggregated = append(slices.partialAggregated, namespace)
	}

	return slices
}

// resolveClusterNamespacesForQueryWithRestrictQueryOptions returns the cluster
// namespace referred to by the restrict fetch options or an error if it
// cannot be found.
func resolveClusterNamespacesForQueryWithRestrictQueryOptions(
	now, start time.Time,
	clusters Clusters,
	restrict storage.RestrictByType,
) (consolidators.QueryFanoutType, ClusterNamespaces, error) {
	coversRangeFilter := newCoversRangeFilter(coversRangeFilterOptions{
		now:        now,
		queryStart: start,
	})

	result := func(
		namespace ClusterNamespace,
		err error,
	) (consolidators.QueryFanoutType, ClusterNamespaces, error) {
		if err != nil {
			return 0, nil, err
		}

		if coversRangeFilter(namespace) {
			return consolidators.NamespaceCoversAllQueryRange,
				ClusterNamespaces{namespace}, nil
		}

		return consolidators.NamespaceCoversPartialQueryRange,
			ClusterNamespaces{namespace}, nil
	}

	switch restrict.MetricsType {
	case storagemetadata.UnaggregatedMetricsType:
		ns, ok := clusters.UnaggregatedClusterNamespace()
		if !ok {
			return result(nil,
				fmt.Errorf("could not find unaggregated namespace for storage policy: %v",
					restrict.StoragePolicy.String()))
		}
		return result(ns, nil)
	case storagemetadata.AggregatedMetricsType:
		ns, ok := clusters.AggregatedClusterNamespace(RetentionResolution{
			Retention:  restrict.StoragePolicy.Retention().Duration(),
			Resolution: restrict.StoragePolicy.Resolution().Window,
		})
		if !ok {
			err := xerrors.NewInvalidParamsError(
				fmt.Errorf("could not find namespace for storage policy: %v",
					restrict.StoragePolicy.String()))
			return result(nil, err)
		}

		return result(ns, nil)
	default:
		err := xerrors.NewInvalidParamsError(
			fmt.Errorf("unrecognized metrics type: %v", restrict.MetricsType))
		return result(nil, err)
	}
}

type coversRangeFilterOptions struct {
	now        time.Time
	queryStart time.Time
}

func newCoversRangeFilter(opts coversRangeFilterOptions) func(namespace ClusterNamespace) bool {
	return func(namespace ClusterNamespace) bool {
		// Include only if can fulfill the entire time range of the query
		clusterStart := opts.now.Add(-1 * namespace.Options().Attributes().Retention)
		return !clusterStart.After(opts.queryStart)
	}
}
