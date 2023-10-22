// Copyright (c) 2016 Uber Technologies, Inc.
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

package storage

import (
	"hash/fnv"
	"sort"
)

func getHash(b []byte) uint64 {
	hash := fnv.New64a()
	hash.Write(b)
	return hash.Sum64()
}

type metricCardinality struct {
	name []byte
	cardinality int
}
func newMetricCardinality(name []byte, cardinality int) (uint64, *metricCardinality) {
	return getHash(name), &metricCardinality{
		name: name,
		cardinality: cardinality,
	}
}
type tickResult struct {
	activeSeries           int
	expiredSeries          int
	activeBlocks           int
	wiredBlocks            int
	unwiredBlocks          int
	pendingMergeBlocks     int
	madeExpiredBlocks      int
	madeUnwiredBlocks      int
	mergedOutOfOrderBlocks int
	errors                 int
	evictedBuckets         int
	// The key is the hash value of the metric name.
	metricToCardinality    map[uint64]*metricCardinality
}

func (r *tickResult) trackTopMetrics() {
	r.metricToCardinality = make(map[uint64]*metricCardinality)
}

func (r *tickResult) truncateTopMetrics(topN int) {
	if topN <= 0 {
		return
	}
	if r.metricToCardinality == nil || len(r.metricToCardinality) <= topN {
		return
	}
	// TODO: use a heap to optimize this.
	cardinalities := make([]int, 0, len(r.metricToCardinality))
	for _, metric := range r.metricToCardinality {
		cardinalities = append(cardinalities, metric.cardinality)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(cardinalities)))
	cutoffValue := cardinalities[topN-1]
	cutoffValueQuota := 1
	for i := topN - 2; i >= 0; i-- {
		if cardinalities[i] == cutoffValue {
			cutoffValueQuota++
		} else {
			break
		}
	}
	for hash, metric := range r.metricToCardinality {
		if metric.cardinality < cutoffValue {
			delete(r.metricToCardinality, hash)
		} else if metric.cardinality == cutoffValue {
			if cutoffValueQuota > 0 {
				cutoffValueQuota--
			} else {
				delete(r.metricToCardinality, hash)
			}
		}
	}
}

// NB: this method modifies the receiver in-place.
func (r *tickResult) merge(other tickResult, topN int) {
	r.activeSeries += other.activeSeries
	r.expiredSeries += other.expiredSeries
	r.activeBlocks += other.activeBlocks
	r.wiredBlocks += other.wiredBlocks
	r.pendingMergeBlocks += other.pendingMergeBlocks
	r.unwiredBlocks += other.unwiredBlocks
	r.madeExpiredBlocks += other.madeExpiredBlocks
	r.madeUnwiredBlocks += other.madeUnwiredBlocks
	r.mergedOutOfOrderBlocks += other.mergedOutOfOrderBlocks
	r.errors += other.errors
	r.evictedBuckets += other.evictedBuckets

	if other.metricToCardinality == nil {
		return
	}
	if r.metricToCardinality == nil {
		r.metricToCardinality = other.metricToCardinality
		return
	}

	for hash, otherMetric := range other.metricToCardinality {
		if currentMetric, ok := r.metricToCardinality[hash]; ok {
			currentMetric.cardinality += otherMetric.cardinality
		} else {
			r.metricToCardinality[hash] = otherMetric
		}
	}

	r.truncateTopMetrics(topN)
}
