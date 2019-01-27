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

package ts

import (
	"math"
	"time"
)

// LTTB down-samples the data to contain only threshold number of points that
// have the same visual shape as the original data. Inspired from
// https://github.com/dgryski/go-lttb which is based on
// https://skemman.is/bitstream/1946/15343/3/SS_MSthesis.pdf
func LTTB(b *Series, start time.Time, end time.Time, millisPerStep int) *Series {
	if end.After(b.EndTime()) {
		end = b.EndTime()
	}

	seriesValuesPerStep := millisPerStep / b.MillisPerStep()
	seriesStart, seriesEnd := b.StepAtTime(start), b.StepAtTime(end)

	// This threshold is different than max datapoints since we ensure step size is an integer multiple of original series step
	threshold := int(math.Ceil(float64(seriesEnd-seriesStart) / float64(seriesValuesPerStep)))
	if threshold == 0 || threshold > b.Len() {
		return b // Nothing to do
	}

	values := NewValues(b.ctx, millisPerStep, threshold)
	// Bucket size. Leave room for start and end data points
	every := float64(seriesValuesPerStep)
	// Always add the first point
	values.SetValueAt(0, b.ValueAt(seriesStart))
	// Set a to be the first chosen point
	a := seriesStart

	bucketStart := seriesStart + 1
	bucketCenter := bucketStart + int(math.Floor(every)) + 1

	for i := 0; i < threshold-2; i++ {
		bucketEnd := bucketCenter + int(math.Floor(every))

		// Calculate point average for next bucket (containing c)
		avgRangeStart := bucketCenter
		avgRangeEnd := bucketEnd

		if avgRangeEnd >= seriesEnd {
			avgRangeEnd = seriesEnd
		}

		avgRangeLength := float64(avgRangeEnd - avgRangeStart)

		var avgX, avgY float64
		var valuesRead int
		for ; avgRangeStart < avgRangeEnd; avgRangeStart++ {
			yVal := b.ValueAt(avgRangeStart)
			if math.IsNaN(yVal) {
				continue
			}
			valuesRead++
			avgX += float64(avgRangeStart)
			avgY += yVal
		}

		if valuesRead > 0 {
			avgX /= avgRangeLength
			avgY /= avgRangeLength
		} else {
			// If all nulls then should not assign a value to average
			avgX = math.NaN()
			avgY = math.NaN()
		}

		// Get the range for this bucket
		rangeOffs := bucketStart
		rangeTo := bucketCenter

		// Point a
		pointAX := float64(a)
		pointAY := b.ValueAt(a)

		var nextA int

		// If all points in left or right bucket are null, then fallback to average
		if math.IsNaN(avgY) || math.IsNaN(pointAY) {
			nextA = indexClosestToAverage(b, rangeOffs, rangeTo)
		} else {
			nextA = indexWithLargestTriangle(b, rangeOffs, rangeTo, pointAX, pointAY, avgX, avgY)
		}

		values.SetValueAt(i+1, b.ValueAt(nextA)) // Pick this point from the bucket
		a = nextA                                // This a is the next a (chosen b)

		bucketStart = bucketCenter
		bucketCenter = bucketEnd
	}

	if values.Len() > 1 {
		// Always add last if not just a single step
		values.SetValueAt(values.Len()-1, b.ValueAt(seriesEnd-1))
	}

	// Derive a new series
	sampledSeries := b.DerivedSeries(start, values)
	return sampledSeries
}

func indexWithLargestTriangle(b *Series, start int, end int, leftX float64, leftY float64, rightX float64, rightY float64) int {
	// The original algorithm implementation initializes the maxArea as 0 which is a bug!
	maxArea := -1.0
	var largestIndex int

	xDifference := leftX - rightX
	yDifference := rightY - leftY
	for index := start; index < end; index++ {
		// Calculate triangle area over three buckets
		area := xDifference*(b.ValueAt(index)-leftY) - (leftX-float64(index))*yDifference
		// We only care about the relative area here.
		area = math.Abs(area)
		// Handle nulls properly
		if math.IsNaN(area) {
			area = 0
		}

		if area > maxArea {
			maxArea = area
			largestIndex = index
		}
	}

	return largestIndex
}

func indexClosestToAverage(b *Series, start int, end int) int {
	var sum float64
	var count int
	for index := start; index < end; index++ {
		if math.IsNaN(b.ValueAt(index)) {
			continue
		}

		sum += b.ValueAt(index)
		count++
	}

	if count == 0 {
		return start
	}

	average := sum / float64(count)
	minDifference := math.MaxFloat64
	closestIndex := start
	for index := start; index < end; index++ {
		difference := math.Abs(average - b.ValueAt(index))
		if !math.IsNaN(b.ValueAt(index)) && difference < minDifference {
			closestIndex = index
			minDifference = difference
		}
	}

	return closestIndex
}
