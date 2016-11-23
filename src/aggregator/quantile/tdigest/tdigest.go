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
	positiveInfinity = math.Inf(1)
	negativeInfinity = math.Inf(-1)
	sentinelCentroid = Centroid{Mean: positiveInfinity, Weight: 0.0}
)

type centroidsByMeanAsc []Centroid

func (c centroidsByMeanAsc) Len() int           { return len(c) }
func (c centroidsByMeanAsc) Less(i, j int) bool { return c[i].Mean < c[j].Mean }
func (c centroidsByMeanAsc) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type tDigest struct {
	compression      float64       // compression factor
	mergedCapacity   int           // merged centroid slice capacity
	unmergedCapacity int           // unmerged centroid slice capacity
	multiplier       int64         // quantile precision multiplier
	centroidsPool    CentroidsPool // centroids pool

	closed         bool       // whether the t-digest is closed
	merged         []Centroid // merged centroids
	mergedWeight   float64    // total weight of merged centroids
	unmerged       []Centroid // unmerged centroid slice capacity
	unmergedWeight float64    // total weight of unmerged centroids
	minValue       float64    // minimum value
	maxValue       float64    // maximum value
}

// mergedCapacity computes the capacity of the merged centroid slice
func mergedCapacity(compression float64) int {
	return int(math.Pi*compression/2 + 0.5)
}

func unmergedCapacity(compression float64) int {
	compression = math.Min(math.Max(20, compression), 1000)
	return (int)(7.5 + 0.37*compression - 2e-4*compression*compression)
}

// NewTDigest creates a new t-digest
// TODO(xichen): add pooling for t-digests
// TODO(xichen): add metrics
func NewTDigest(opts Options) TDigest {
	centroidsPool := opts.CentroidsPool()
	compression := opts.CompressionFactor()
	mergedCapacity := mergedCapacity(compression)
	unmergedCapacity := unmergedCapacity(compression)

	var multiplier int64
	if precision := opts.QuantilePrecision(); precision != 0 {
		multiplier = int64(math.Pow10(precision))
	}

	return &tDigest{
		compression:      opts.CompressionFactor(),
		multiplier:       multiplier,
		centroidsPool:    centroidsPool,
		merged:           centroidsPool.Get(mergedCapacity),
		mergedCapacity:   mergedCapacity,
		unmerged:         centroidsPool.Get(unmergedCapacity),
		unmergedCapacity: unmergedCapacity,
		minValue:         positiveInfinity,
		maxValue:         negativeInfinity,
	}
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

func (d *tDigest) Quantile(q float64) float64 {
	// compress the centroids first
	d.compress()

	var (
		targetWeight = q * d.mergedWeight
		curWeight    = 0.0
		lowerBound   = d.minValue
		upperBound   float64
	)
	for i, c := range d.merged {
		upperBound = d.upperBound(i)
		if targetWeight <= curWeight+c.Weight {
			// The quantile falls within this centroid
			ratio := (targetWeight - curWeight) / c.Weight
			quantile := lowerBound + ratio*(upperBound-lowerBound)
			// If there is a desired precision, we truncate the quantile per the precision requirement
			if d.multiplier != 0 {
				quantile = math.Trunc(quantile*float64(d.multiplier)) / float64(d.multiplier)
			}
			return quantile
		}
		curWeight += c.Weight
		lowerBound = upperBound
	}

	// NB(xichen): should never get here unless the centroids array are empty
	// because the target weight should always be no larger than the total weight
	return math.NaN()
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

// compress merges unmerged centroids and merged centroids in place
// using unmerged centroids as temporary buffer
func (d *tDigest) compress() {
	if len(d.unmerged) == 0 {
		return
	}

	sort.Sort(centroidsByMeanAsc(d.unmerged))

	var (
		totalWeight = d.mergedWeight + d.unmergedWeight
		currWeight  = 0.0
		currIndex   = 0.0
		merged      = d.merged
		unmerged    = d.unmerged
		buffer      = d.unmerged[:0]
	)

	d.merged = d.merged[:0]
	for len(merged) > 0 || len(buffer) > 0 || len(unmerged) > 0 {
		currUnmerged := sentinelCentroid
		if len(unmerged) > 0 {
			currUnmerged = unmerged[0]
		}

		currMerged := sentinelCentroid
		// buffer takes precedence over merged
		if len(buffer) > 0 {
			currMerged = buffer[0]
		} else if len(merged) > 0 {
			currMerged = merged[0]
		}

		if currUnmerged.Mean < currMerged.Mean {
			// Save the first centroid in case it'll be overwritten
			if len(merged) > 0 {
				buffer = d.appendCentroid(buffer, merged[0])
				merged = merged[1:]
			}
			unmerged = unmerged[1:]
			currIndex = d.mergeCentroid(currIndex, currWeight, totalWeight, currUnmerged)
			currWeight += currUnmerged.Weight
		} else {
			if len(merged) > 0 {
				// Save the first centroid in case it'll be overwritten
				if len(buffer) > 0 {
					copy(buffer, buffer[1:])
					buffer[len(buffer)-1] = merged[0]
				}
				merged = merged[1:]
			} else {
				// Merged buffer is used up, now consuming saved centroids
				buffer = buffer[1:]
			}
			currIndex = d.mergeCentroid(currIndex, currWeight, totalWeight, currMerged)
			currWeight += currMerged.Weight
		}
	}

	d.unmerged = d.unmerged[:0]
	d.unmergedWeight = 0.0
	d.mergedWeight = totalWeight
}

// mergeCentroid merges a centroid into the list of merged centroids
func (d *tDigest) mergeCentroid(
	currIndex float64,
	currWeight float64,
	totalWeight float64,
	c Centroid,
) float64 {
	nextIndex := d.nextIndex((currWeight + c.Weight) / totalWeight)
	if nextIndex-currIndex > 1 || len(d.merged) == 0 {
		// This is the first centroid added, or the next index is too far away from the current index
		d.merged = d.appendCentroid(d.merged, c)
		return d.nextIndex(currWeight / totalWeight)
	}

	// The new centroid falls within the range of the current centroid
	numMerged := len(d.merged)
	d.merged[numMerged-1].Weight += c.Weight
	d.merged[numMerged-1].Mean += (c.Mean - d.merged[numMerged-1].Mean) * c.Weight / d.merged[numMerged-1].Weight
	return currIndex
}

// nextIndex estimates the index of the next centroid
func (d *tDigest) nextIndex(quantile float64) float64 {
	return d.compression * (math.Asin(2*quantile-1)/math.Pi + 0.5)
}

// add adds a weighted value
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
		n := copy(newCentroids, centroids)
		d.centroidsPool.Put(centroids)
		centroids = newCentroids[:n]
	}
	return append(centroids, c)
}
