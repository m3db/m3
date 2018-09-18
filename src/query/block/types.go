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
	"math"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
)

// Block represents a group of series across a time bound
type Block interface {
	// Unconsolidated returns the unconsolidated version of the block
	Unconsolidated() (UnconsolidatedBlock, error)
	// StepIter returns a StepIterator
	StepIter() (StepIter, error)
	// SeriesIter returns a SeriesIterator
	SeriesIter() (SeriesIter, error)
	// Close frees up any resources
	Close() error
}

// UnconsolidatedBlock represents a group of unconsolidated series across a time bound
type UnconsolidatedBlock interface {
	// StepIter returns a StepIterator
	StepIter() (UnconsolidatedStepIter, error)
	// SeriesIter returns a SeriesIterator
	SeriesIter() (UnconsolidatedSeriesIter, error)
	// Consolidate an unconsolidated block
	Consolidate() (Block, error)
	// Close frees up any resources
	Close() error
}

// SeriesMeta is metadata data for the series
type SeriesMeta struct {
	Tags models.Tags
	Name string
}

// Iterator is the base iterator
type Iterator interface {
	Next() bool
	Close()
}

// MetaIter is implemented by iterators which provide meta information
type MetaIter interface {
	// SeriesMeta returns the metadata for each series in the block
	SeriesMeta() []SeriesMeta
	// Meta returns the metadata for the block
	Meta() Metadata
}

// SeriesMetaIter is implemented by series iterators which provide meta information
type SeriesMetaIter interface {
	MetaIter
	// SeriesCount returns the number of series
	SeriesCount() int
}

// SeriesIter iterates through a block horizontally
type SeriesIter interface {
	Iterator
	SeriesMetaIter
	// Current returns the current series for the block
	Current() (Series, error)
}

// UnconsolidatedSeriesIter iterates through a block horizontally
type UnconsolidatedSeriesIter interface {
	Iterator
	SeriesMetaIter
	// Current returns the current series for the block
	Current() (UnconsolidatedSeries, error)
}

// StepMetaIter is implemented by step iterators which provide meta information
type StepMetaIter interface {
	MetaIter
	// StepCount returns the number of steps
	StepCount() int
}

// StepIter iterates through a block vertically
type StepIter interface {
	Iterator
	StepMetaIter
	// Current returns the current step for the block
	Current() (Step, error)
}

// UnconsolidatedStepIter iterates through a block vertically
type UnconsolidatedStepIter interface {
	Iterator
	StepMetaIter
	// Current returns the current step for the block
	Current() (UnconsolidatedStep, error)
}

// Step is a single time step within a block
type Step interface {
	Time() time.Time
	Values() []float64
}

// UnconsolidatedStep is a single unconsolidated time step within a block
type UnconsolidatedStep interface {
	Time() time.Time
	Values() []ts.Datapoints
}

// Metadata is metadata for a block
type Metadata struct {
	Bounds models.Bounds
	Tags   models.Tags // Common tags across different series
}

// String returns a string representation of metadata
func (m Metadata) String() string {
	return fmt.Sprintf("Bounds: %v, Tags: %v", m.Bounds, m.Tags)
}

// Builder builds a new block
type Builder interface {
	AppendValue(idx int, value float64) error
	AppendValues(idx int, values []float64) error
	Build() Block
	AddCols(num int) error
}

// Result is the result from a block query
type Result struct {
	Blocks []Block
}

// ConsolidationFunc consolidates a bunch of datapoints into a single float value
type ConsolidationFunc func(datapoints ts.Datapoints) float64

// TakeLast is a consolidation function which takes the last datapoint
func TakeLast(values ts.Datapoints) float64 {
	if len(values) == 0 {
		return math.NaN()
	}

	return values[len(values)-1].Value
}
