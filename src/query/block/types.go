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

package block

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/models"
)

// Block represents a group of series across a time bound
type Block interface {
	// StepIter returns a StepIterator
	StepIter() (StepIter, error)
	// SeriesIter returns a SeriesIterator
	SeriesIter() (SeriesIter, error)
	// Close frees up any resources
	Close() error
}

// SeriesMeta is metadata data for the series
type SeriesMeta struct {
	Tags models.Tags
	Name string
}

// Bounds are the time bounds
type Bounds struct {
	Start    time.Time
	End      time.Time
	StepSize time.Duration
}

// TimeForIndex returns the start time for a given index assuming a uniform step size
func (b Bounds) TimeForIndex(idx int) (time.Time, error) {
	step := b.StepSize
	t := b.Start.Add(time.Duration(idx) * step)
	if t.After(b.End) {
		return time.Time{}, fmt.Errorf("out of bounds, %d", idx)
	}

	return t, nil
}

// Steps calculates the number of steps for the bounds
func (b Bounds) Steps() int {
	if b.Start.After(b.End) || b.StepSize <= 0 {
		return 0
	}

	return int(b.End.Sub(b.Start)/b.StepSize) + 1
}

// String representation of the bounds
func (b Bounds) String() string {
	return fmt.Sprintf("start: %v, end: %v, stepSize: %v, steps: %d", b.Start, b.End, b.StepSize, b.Steps())
}

// Iterator is the base iterator
type Iterator interface {
	Next() bool
	Close()
}

// SeriesIter iterates through a block horizontally
type SeriesIter interface {
	Iterator
	// Current returns the current series for the block
	Current() (Series, error)
	// SeriesCount returns the number of series
	SeriesCount() int
	// SeriesMeta returns the metadata for each series in the block
	SeriesMeta() []SeriesMeta
	// Meta returns the metadata for the block
	Meta() Metadata
}

// StepIter iterates through a block vertically
type StepIter interface {
	Iterator
	// Current returns the current step for the block
	Current() (Step, error)
	// StepCount returns the number of steps
	StepCount() int
	// SeriesMeta returns the metadata for each series in the block
	SeriesMeta() []SeriesMeta
	// Meta returns the metadata for the block
	Meta() Metadata
}

// Step is a single time step within a block
type Step interface {
	Time() time.Time
	Values() []float64
}

// Metadata is metadata for a block
type Metadata struct {
	Bounds Bounds
	Tags   models.Tags // Common tags across different series
}

// String returns a string representation of metadata
func (m Metadata) String() string {
	return fmt.Sprintf("Bounds: %v, Tags: %v", m.Bounds, m.Tags)
}

// Builder builds a new block
type Builder interface {
	AppendValue(idx int, value float64) error
	Build() Block
	AddCols(num int) error
}

// Result is the result from a block query
type Result struct {
	Blocks []Block
}
