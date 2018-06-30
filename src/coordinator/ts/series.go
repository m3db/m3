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

package ts

import (
	"fmt"
	"time"

	"github.com/m3db/m3db/src/coordinator/errors"
	"github.com/m3db/m3db/src/coordinator/models"
)

// Series is the public interface to a block of timeseries values.  Each block has a start time,
// a logical number of steps, and a step size indicating the number of milliseconds represented by each point.
type Series struct {
	name string
	vals Values
	Tags models.Tags
}

// NewSeries creates a new Series at a given start time, backed by the provided values
func NewSeries(name string, vals Values, tags models.Tags) *Series {
	return &Series{
		name: name,
		vals: vals,
		Tags: tags,
	}
}

// Name returns the name of the timeseries block
func (s *Series) Name() string { return s.name }

// Len returns the number of values in the time series. Used for aggregation
func (s *Series) Len() int { return s.vals.Len() }

// Values returns the underlying values interface
func (s *Series) Values() Values { return s.vals }

// Align adjusts the datapoints to start, end and a fixed interval
func (s *Series) Align(start, end time.Time, interval time.Duration) (*Series, error) {
	fixedVals, err := alignValues(s.Values(), start, end, interval)
	if err != nil {
		return nil, err
	}

	return NewSeries(s.name, fixedVals, s.Tags), nil
}

func alignValues(values Values, start, end time.Time, interval time.Duration) (FixedResolutionMutableValues, error) {
	switch vals := values.(type) {
	case Datapoints:
		return RawPointsToFixedStep(vals, start, end, interval)
	case FixedResolutionMutableValues:
		// TODO: Align fixed resolution as well once storages can return those directly
		return vals, nil
	default:
		return nil, fmt.Errorf("unknown type: %v", vals)
	}
}

// SeriesList represents a slice of series pointers
type SeriesList []*Series

// Resolution returns the resolution for a fixed step series list. It assumes all underlying series will have the same resolution
func (seriesList SeriesList) Resolution() (time.Duration, error) {
	var resolution time.Duration
	for i, s := range seriesList {
		fixedRes, ok := s.Values().(FixedResolutionMutableValues)
		if !ok {
			return 0, errors.ErrOnlyFixedResSupported
		}

		if i == 0 {
			resolution = fixedRes.Resolution()
		} else {
			if resolution != fixedRes.Resolution() {
				return 0, fmt.Errorf("resolution mismatch, r1: %v, r2: %v", resolution, fixedRes.Resolution())
			}
		}
	}

	return resolution, nil
}

// Align aligns each series to the given start, end and step.
func (seriesList SeriesList) Align(start, end time.Time, interval time.Duration) (SeriesList, error) {
	alignedList := make(SeriesList, len(seriesList))
	for i, s := range seriesList {
		alignedSeries, err := s.Align(start, end, interval)
		if err != nil {
			return nil, err
		}

		alignedList[i] = alignedSeries
	}

	return alignedList, nil
}
