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
	"strings"
	"time"

	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	xtime "github.com/m3db/m3/src/x/time"
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
}

type resolvedNamespace struct {
	ClusterNamespace

	narrowing consolidators.Narrowing
}

func resolved(ns ClusterNamespace) resolvedNamespace {
	return resolvedNamespace{ClusterNamespace: ns}
}

// resolveUnaggregatedNamespaceForQuery determines if the unaggregated namespace
// should be used, and if so, determines if it fully satisfies the query range.
func resolveUnaggregatedNamespaceForQuery(
	now, start xtime.UnixNano,
	unaggregated ClusterNamespace,
	opts *storage.FanoutOptions,
) unaggregatedNamespaceDetails {
	if opts.FanoutUnaggregated == storage.FanoutForceDisable {
		return unaggregatedNamespaceDetails{satisfies: disabled}
	}

	var (
		retention         = unaggregated.Options().Attributes().Retention
		unaggregatedStart = now.Add(-1 * retention)
	)

	satisfies := fullySatisfiesRange
	if unaggregatedStart.After(start) {
		satisfies = partiallySatisfiesRange
	}

	return unaggregatedNamespaceDetails{
		clusterNamespace: unaggregated,
		satisfies:        satisfies,
	}
}

// resolveClusterNamespacesForQuery returns the namespaces that need to be
// fanned out to depending on the query time and the namespaces configured.
func resolveClusterNamespacesForQuery(
	now,
	start,
	end xtime.UnixNano,
	clusters Clusters,
	opts *storage.FanoutOptions,
	restrict *storage.RestrictQueryOptions,
	relatedQueryOpts *storage.RelatedQueryOptions,
) (consolidators.QueryFanoutType, []resolvedNamespace, error) {
	// Calculate a new Start time if related query opts are present.
	// NB: We do not calculate a new End time because it does not factor
	// into namespace selection.
	namespaceSelectionStart := start
	if relatedQueryOpts != nil {
		for _, timeRange := range relatedQueryOpts.Timespans {
			if timeRange.Start < namespaceSelectionStart {
				namespaceSelectionStart = timeRange.Start
			}
		}
	}

	// 1. First resolve the logical plan.
	fanout, namespaces, err := resolveClusterNamespacesForQueryLogicalPlan(now,
		namespaceSelectionStart, end, clusters, opts, restrict)
	if err != nil {
		return fanout, namespaces, err
	}

	// 2. Create physical plan.
	// Now de-duplicate any namespaces that might be fetched twice due to
	// the fact some of the same namespaces are reused once for unaggregated
	// and another for aggregated rollups (which don't collide with timeseries).
	filtered := namespaces[:0]
	for _, ns := range namespaces {
		keep := true
		// Small enough that we can do n^2 here instead of creating a map,
		// usually less than 4 namespaces resolved.
		for _, existing := range filtered {
			if ns.NamespaceID().Equal(existing.NamespaceID()) {
				keep = false
				break
			}
		}
		if !keep {
			continue
		}
		filtered = append(filtered, ns)
	}

	return fanout, filtered, nil
}

// resolveClusterNamespacesForQueryLogicalPlan resolves the logical plan
// for namespaces to query.
// nolint: unparam
func resolveClusterNamespacesForQueryLogicalPlan(
	now, start, end xtime.UnixNano,
	clusters Clusters,
	opts *storage.FanoutOptions,
	restrict *storage.RestrictQueryOptions,
) (consolidators.QueryFanoutType, []resolvedNamespace, error) {
	if typeRestrict := restrict.GetRestrictByType(); typeRestrict != nil {
		// If a specific restriction is set, then attempt to satisfy.
		return resolveClusterNamespacesForQueryWithTypeRestrictQueryOptions(now,
			start, clusters, *typeRestrict)
	}

	if typesRestrict := restrict.GetRestrictByTypes(); typesRestrict != nil {
		// If a specific restriction is set, then attempt to satisfy.
		return resolveClusterNamespacesForQueryWithTypesRestrictQueryOptions(now,
			start, clusters, typesRestrict)
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
			[]resolvedNamespace{resolved(unaggregated.clusterNamespace)},
			nil
	}

	if opts.FanoutAggregated == storage.FanoutForceDisable {
		if unaggregated.satisfies == partiallySatisfiesRange {
			return consolidators.NamespaceCoversPartialQueryRange,
				[]resolvedNamespace{resolved(unaggregated.clusterNamespace)}, nil
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
	r = aggregatedNamespaces(clusters.ClusterNamespaces(), r, coversRangeFilter, now, end, opts)

	// If any of the aggregated clusters have a complete set of metrics, use
	// those that have the smallest resolutions, supplemented by lower resolution
	// partially aggregated metrics.
	if len(r.completeAggregated) > 0 {
		sort.Stable(resolvedNamespacesByResolutionAsc(r.completeAggregated))
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

		if !result[0].narrowing.End.IsZero() {
			// completeAggregated namespace will not have the most recent data available, will
			// have to query unaggregated namespace for it and then stitch the responses together.
			unaggregatedNarrowed := resolved(unaggregated.clusterNamespace)
			unaggregatedNarrowed.narrowing.Start = result[0].narrowing.End

			result = append(result, unaggregatedNarrowed)
		}

		return consolidators.NamespaceCoversAllQueryRange, result, nil
	}

	// No complete aggregated namespaces can definitely fulfill the query,
	// so take the longest retention completed aggregated namespace to return
	// as much data as possible, along with any partially aggregated namespaces
	// that have either same retention and lower resolution or longer retention
	// than the complete aggregated namespace.
	r = aggregatedNamespaces(clusters.ClusterNamespaces(), r, nil, now, end, opts)
	if len(r.completeAggregated) == 0 {
		// Absolutely no complete aggregated namespaces, need to fanout to all
		// partial aggregated namespaces as well as the unaggregated cluster
		// as we have no idea which has the longest retention.
		result := r.partialAggregated
		// If unaggregated namespace can partially satisfy this range, add it as a
		// fanout contender.
		if unaggregated.satisfies == partiallySatisfiesRange {
			result = append(result, resolved(unaggregated.clusterNamespace))
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
	sort.Stable(sort.Reverse(resolvedNamespacesByRetentionAsc(r.completeAggregated)))

	// Take longest retention complete aggregated namespace or the unaggregated
	// cluster if that is longer than the longest aggregated namespace.
	result := r.completeAggregated[:1]
	completedAttrs := result[0].Options().Attributes()
	if unaggregated.satisfies == partiallySatisfiesRange {
		unaggregatedAttrs := unaggregated.clusterNamespace.Options().Attributes()
		if completedAttrs.Retention <= unaggregatedAttrs.Retention {
			// If the longest aggregated cluster for some reason has lower retention
			// than the unaggregated cluster then we prefer the unaggregated cluster
			// as it has a complete data set and is always the most granular.
			result[0] = resolved(unaggregated.clusterNamespace)
			completedAttrs = unaggregated.clusterNamespace.Options().Attributes()
		}
	}

	if !result[0].narrowing.End.IsZero() {
		// completeAggregated namespace will not have the most recent data available, will
		// have to query unaggregated namespace for it and then stitch the responses together.
		unaggregatedNarrowed := resolved(unaggregated.clusterNamespace)
		unaggregatedNarrowed.narrowing.Start = result[0].narrowing.End

		result = append(result, unaggregatedNarrowed)
	}

	// Take any partially aggregated namespaces with longer retention or
	// same retention with more granular resolution that may contain
	// a matching metric.
	for _, n := range r.partialAggregated {
		attrs := n.Options().Attributes()
		if attrs.Retention > completedAttrs.Retention {
			// Higher retention.
			result = append(result, n)
		} else if attrs.Retention == completedAttrs.Retention &&
			attrs.Resolution < completedAttrs.Resolution {
			// Same retention but more granular resolution.
			result = append(result, n)
		}
	}

	return consolidators.NamespaceCoversPartialQueryRange, result, nil
}

type reusedAggregatedNamespaceSlices struct {
	completeAggregated []resolvedNamespace
	partialAggregated  []resolvedNamespace
}

func (slices reusedAggregatedNamespaceSlices) reset(
	size int,
) reusedAggregatedNamespaceSlices {
	// Initialize arrays if yet uninitialized.
	if slices.completeAggregated == nil {
		slices.completeAggregated = make([]resolvedNamespace, 0, size)
	} else {
		slices.completeAggregated = slices.completeAggregated[:0]
	}

	if slices.partialAggregated == nil {
		slices.partialAggregated = make([]resolvedNamespace, 0, size)
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
	now, end xtime.UnixNano,
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

		resolvedNs := resolved(namespace)

		var dataLatency time.Duration
		downsampled := strings.HasPrefix(resolvedNs.NamespaceID().String(), "downsampled")
		if downsampled {
			// FIXME: make this configurable
			dataLatency = 12 * time.Hour
		}

		dataAvailableUntil := now.Add(-dataLatency)
		if dataLatency > 0 && end.After(dataAvailableUntil) {
			resolvedNs.narrowing.End = dataAvailableUntil
		}

		// If not optimizing fanout to aggregated namespaces, set all aggregated
		// namespaces satisfying the filter as partially aggregated, as all metrics
		// do not necessarily appear in all namespaces, depending on configuration.
		if opts.FanoutAggregatedOptimized == storage.FanoutForceDisable {
			slices.partialAggregated = append(slices.partialAggregated, resolvedNs)
			continue
		}

		// Otherwise, check downsample options for the namespace and determine if
		// this namespace is set as containing all metrics.
		downsampleOpts, err := nsOpts.DownsampleOptions()
		if err != nil {
			continue
		}

		if downsampleOpts.All || downsampled /*FIXME: make this configurable*/ {
			// This namespace has a complete set of metrics. Ensure that it passes
			// the filter if it was a forced addition, otherwise it may be too short
			// to cover the entire range and should be considered a partial result.
			slices.completeAggregated = append(slices.completeAggregated, resolvedNs)
			continue
		}

		// This namespace does not necessarily have a complete set of metrics.
		slices.partialAggregated = append(slices.partialAggregated, resolvedNs)
	}

	return slices
}

// resolveClusterNamespacesForQueryWithTypeRestrictQueryOptions returns the cluster
// namespace referred to by the restrict fetch options or an error if it
// cannot be found.
func resolveClusterNamespacesForQueryWithTypeRestrictQueryOptions(
	now, start xtime.UnixNano,
	clusters Clusters,
	restrict storage.RestrictByType,
) (consolidators.QueryFanoutType, []resolvedNamespace, error) {
	coversRangeFilter := newCoversRangeFilter(coversRangeFilterOptions{
		now:        now,
		queryStart: start,
	})

	result := func(
		namespace ClusterNamespace,
		err error,
	) (consolidators.QueryFanoutType, []resolvedNamespace, error) {
		if err != nil {
			return 0, nil, err
		}

		if coversRangeFilter(namespace) {
			return consolidators.NamespaceCoversAllQueryRange,
				[]resolvedNamespace{resolved(namespace)}, nil
		}

		return consolidators.NamespaceCoversPartialQueryRange,
			[]resolvedNamespace{resolved(namespace)}, nil
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

// resolveClusterNamespacesForQueryWithTypesRestrictQueryOptions returns the cluster
// namespace referred to by the array of restrict fetch options or an error if it
// cannot be found.
func resolveClusterNamespacesForQueryWithTypesRestrictQueryOptions(
	now, start xtime.UnixNano,
	clusters Clusters,
	restricts []*storage.RestrictByType,
) (consolidators.QueryFanoutType, []resolvedNamespace, error) {
	var (
		namespaces []resolvedNamespace
		fanoutType consolidators.QueryFanoutType
	)
	for _, restrict := range restricts {
		t, ns, err := resolveClusterNamespacesForQueryWithTypeRestrictQueryOptions(now, start, clusters, *restrict)
		if err != nil {
			return consolidators.NamespaceInvalid, nil, err
		}
		namespaces = append(namespaces, ns...)
		if t == consolidators.NamespaceCoversPartialQueryRange ||
			fanoutType == consolidators.NamespaceCoversPartialQueryRange {
			fanoutType = consolidators.NamespaceCoversPartialQueryRange
		} else {
			fanoutType = consolidators.NamespaceCoversAllQueryRange
		}
	}
	return fanoutType, namespaces, nil
}

type coversRangeFilterOptions struct {
	now        xtime.UnixNano
	queryStart xtime.UnixNano
}

func newCoversRangeFilter(opts coversRangeFilterOptions) func(namespace ClusterNamespace) bool {
	return func(namespace ClusterNamespace) bool {
		// Include only if can fulfill the entire time range of the query
		clusterStart := opts.now.Add(-1 * namespace.Options().Attributes().Retention)
		return !clusterStart.After(opts.queryStart)
	}
}

type resolvedNamespacesByResolutionAsc []resolvedNamespace

func (a resolvedNamespacesByResolutionAsc) Len() int      { return len(a) }
func (a resolvedNamespacesByResolutionAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a resolvedNamespacesByResolutionAsc) Less(i, j int) bool {
	return a[i].Options().Attributes().Resolution < a[j].Options().Attributes().Resolution
}

type resolvedNamespacesByRetentionAsc []resolvedNamespace

func (a resolvedNamespacesByRetentionAsc) Len() int      { return len(a) }
func (a resolvedNamespacesByRetentionAsc) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a resolvedNamespacesByRetentionAsc) Less(i, j int) bool {
	return a[i].Options().Attributes().Retention < a[j].Options().Attributes().Retention
}
