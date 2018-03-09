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

package tdigest

import (
	"math"
	"sort"
)

var (
	nan              = math.NaN()
	positiveInfinity = math.Inf(1)
	negativeInfinity = math.Inf(-1)
	sentinelCentroid = Centroid{Mean: positiveInfinity, Weight: 0.0}
)

type centroidsByMeanAsc []Centroid

func (c centroidsByMeanAsc) Len() int           { return len(c) }
func (c centroidsByMeanAsc) Less(i, j int) bool { return c[i].Mean < c[j].Mean }
func (c centroidsByMeanAsc) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type mergeCentroidFn func(
	currIndex float64,
	currWeight float64,
	totalWeight float64,
	c Centroid,
	mergeResult []Centroid,
) (float64, []Centroid)

type tDigest struct {
	compression      float64         // compression factor
	mergedCapacity   int             // merged centroid slice capacity
	unmergedCapacity int             // unmerged centroid slice capacity
	multiplier       int64           // quantile precision multiplier
	mergeCentroidFn  mergeCentroidFn // function to merge centroids
	centroidsPool    CentroidsPool   // centroids pool

	closed         bool       // whether the t-digest is closed
	merged         []Centroid // merged centroids
	mergedWeight   float64    // total weight of merged centroids
	unmerged       []Centroid // unmerged centroid slice capacity
	unmergedWeight float64    // total weight of unmerged centroids
	minValue       float64    // minimum value
	maxValue       float64    // maximum value
}

// mergedCapacity computes the capacity of the merged centroid slice.
func mergedCapacity(compression float64) int {
	return int(math.Ceil(math.Pi*compression + 0.5))
}

func unmergedCapacity(compression float64) int {
	// NB: the formula is taken from tdunning's implementation by
	// regressing against known sizes for sample compression values.
	compression = math.Min(math.Max(20, compression), 1000)
	return int(7.5 + 0.37*compression - 2e-4*compression*compression)
}

// NewTDigest creates a new t-digest.
// TODO(xichen): add pooling for t-digests
// TODO(xichen): add metrics
func NewTDigest(opts Options) TDigest {
	centroidsPool := opts.CentroidsPool()
	compression := opts.Compression()
	mergedCapacity := mergedCapacity(compression)
	unmergedCapacity := unmergedCapacity(compression)

	var multiplier int64
	if precision := opts.Precision(); precision != 0 {
		multiplier = int64(math.Pow10(precision))
	}

	d := &tDigest{
		compression:      opts.Compression(),
		multiplier:       multiplier,
		centroidsPool:    centroidsPool,
		mergedCapacity:   mergedCapacity,
		unmergedCapacity: unmergedCapacity,
	}
	d.mergeCentroidFn = d.mergeCentroid

	d.Reset()
	return d
}

func (d *tDigest) Merged() []Centroid {
	return d.merged
}

func (d *tDigest) Unmerged() []Centroid {
	return d.unmerged
}

func (d *tDigest) Add(value float64) {
	d.add(value, 1.0)
}

func (d *tDigest) Min() float64 {
	return d.Quantile(0.0)
}

func (d *tDigest) Max() float64 {
	return d.Quantile(1.0)
}

func (d *tDigest) Quantile(q float64) float64 {
	if q < 0.0 || q > 1.0 {
		return nan
	}

	// compress the centroids first.
	d.compress()

	// If the digest is empty, return 0.
	if len(d.merged) == 0 {
		return 0.0
	}

	if q == 0.0 {
		return d.minValue
	}

	if q == 1.0 {
		return d.maxValue
	}

	var (
		targetWeight = q * d.mergedWeight
		currWeight   = 0.0
		lowerBound   = d.minValue
		upperBound   float64
	)
	for i, c := range d.merged {
		upperBound = d.upperBound(i)
		if targetWeight <= currWeight+c.Weight {
			// The quantile falls within this centroid.
			ratio := (targetWeight - currWeight) / c.Weight
			quantile := lowerBound + ratio*(upperBound-lowerBound)
			// If there is a desired precision, we truncate the quantile per the precision requirement.
			if d.multiplier != 0 {
				quantile = math.Trunc(quantile*float64(d.multiplier)) / float64(d.multiplier)
			}
			return quantile
		}
		currWeight += c.Weight
		lowerBound = upperBound
	}

	// NB(xichen): should never get here unless the centroids array are empty
	// because the target weight should always be no larger than the total weight.
	return nan
}

func (d *tDigest) Merge(tdigest TDigest) {
	merged := tdigest.Merged()
	for _, c := range merged {
		d.add(c.Mean, c.Weight)
	}

	unmerged := tdigest.Unmerged()
	for _, c := range unmerged {
		d.add(c.Mean, c.Weight)
	}
}

func (d *tDigest) Close() {
	if d.closed {
		return
	}
	d.closed = true
	d.centroidsPool.Put(d.merged)
	d.centroidsPool.Put(d.unmerged)
}

func (d *tDigest) Reset() {
	d.closed = false
	d.merged = d.centroidsPool.Get(d.mergedCapacity)
	d.mergedWeight = 0.0
	d.unmerged = d.centroidsPool.Get(d.unmergedCapacity)
	d.unmergedWeight = 0.0
	d.minValue = positiveInfinity
	d.maxValue = negativeInfinity
}

// compress merges unmerged centroids and merged centroids.
func (d *tDigest) compress() {
	if len(d.unmerged) == 0 {
		return
	}

	sort.Sort(centroidsByMeanAsc(d.unmerged))

	var (
		totalWeight   = d.mergedWeight + d.unmergedWeight
		currWeight    = 0.0
		currIndex     = 0.0
		mergedIndex   = 0
		unmergedIndex = 0
		mergeResult   = d.centroidsPool.Get(len(d.merged) + len(d.unmerged))
	)

	for mergedIndex < len(d.merged) || unmergedIndex < len(d.unmerged) {
		currUnmerged := sentinelCentroid
		if unmergedIndex < len(d.unmerged) {
			currUnmerged = d.unmerged[unmergedIndex]
		}

		currMerged := sentinelCentroid
		if mergedIndex < len(d.merged) {
			currMerged = d.merged[mergedIndex]
		}

		if currUnmerged.Mean < currMerged.Mean {
			currIndex, mergeResult = d.mergeCentroidFn(currIndex, currWeight, totalWeight, currUnmerged, mergeResult)
			currWeight += currUnmerged.Weight
			unmergedIndex++
		} else {
			currIndex, mergeResult = d.mergeCentroidFn(currIndex, currWeight, totalWeight, currMerged, mergeResult)
			currWeight += currMerged.Weight
			mergedIndex++
		}
	}

	d.centroidsPool.Put(d.merged)
	d.merged = mergeResult
	d.mergedWeight = totalWeight
	d.unmerged = d.unmerged[:0]
	d.unmergedWeight = 0.0
}

// mergeCentroid merges a centroid into the list of merged centroids.
func (d *tDigest) mergeCentroid(
	currIndex float64,
	currWeight float64,
	totalWeight float64,
	c Centroid,
	mergeResult []Centroid,
) (float64, []Centroid) {
	nextIndex := d.nextIndex((currWeight + c.Weight) / totalWeight)
	if nextIndex-currIndex > 1 || len(mergeResult) == 0 {
		// This is the first centroid added, or the next index is too far away from the current index.
		mergeResult = d.appendCentroid(mergeResult, c)
		return d.nextIndex(currWeight / totalWeight), mergeResult
	}

	// The new centroid falls within the range of the current centroid.
	numResults := len(mergeResult)
	mergeResult[numResults-1].Weight += c.Weight
	mergeResult[numResults-1].Mean += (c.Mean - mergeResult[numResults-1].Mean) * c.Weight / mergeResult[numResults-1].Weight
	return currIndex, mergeResult
}

// nextIndex estimates the index of the next centroid.
func (d *tDigest) nextIndex(quantile float64) float64 {
	return d.compression * (math.Asin(2*quantile-1)/math.Pi + 0.5)
}

// add adds a weighted value.
func (d *tDigest) add(value float64, weight float64) {
	if len(d.unmerged) == d.unmergedCapacity {
		d.compress()
	}
	d.minValue = math.Min(d.minValue, value)
	d.maxValue = math.Max(d.maxValue, value)
	d.unmerged = d.appendCentroid(d.unmerged, Centroid{Mean: value, Weight: weight})
	d.unmergedWeight += weight
}

// upperBound returns the upper bound for computing quantiles given the centroid index.
// d.merged is guaranteed to have at least one centroid when upperBound is called.
func (d *tDigest) upperBound(index int) float64 {
	if index == len(d.merged)-1 {
		return d.maxValue
	}
	return (d.merged[index].Mean + d.merged[index+1].Mean) / 2.0
}

func (d *tDigest) appendCentroid(centroids []Centroid, c Centroid) []Centroid {
	if len(centroids) == cap(centroids) {
		newCentroids := d.centroidsPool.Get(2 * len(centroids))
		newCentroids = append(newCentroids, centroids...)
		d.centroidsPool.Put(centroids)
		centroids = newCentroids
	}
	return append(centroids, c)
}
