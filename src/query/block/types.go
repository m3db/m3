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
	"io"
	"math"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
)

// Block represents a group of series across a time bound.
type Block interface {
	io.Closer
	// Unconsolidated returns the unconsolidated version of the block.
	Unconsolidated() (UnconsolidatedBlock, error)
	// StepIter returns a step-wise block iterator, giving consolidated values
	// across all series comprising the box at a single time step.
	StepIter() (StepIter, error)
	// SeriesIter returns a series-wise block iterator, giving consolidated values
	// by series.
	SeriesIter() (SeriesIter, error)
	// WithMetadata returns a block with updated meta and series metadata.
	WithMetadata(Metadata, []SeriesMeta) (Block, error)
}

// UnconsolidatedBlock represents a group of unconsolidated series across a time bound
type UnconsolidatedBlock interface {
	io.Closer
	// StepIter returns a step-wise block iterator, giving unconsolidated values
	// across all series comprising the box at a single time step.
	StepIter() (UnconsolidatedStepIter, error)
	// SeriesIter returns a series-wise block iterator, giving unconsolidated
	// by series.
	SeriesIter() (UnconsolidatedSeriesIter, error)
	// Consolidate attempts to consolidate the unconsolidated block.
	Consolidate() (Block, error)
	// WithMetadata returns a block with updated meta and series metadata.
	WithMetadata(Metadata, []SeriesMeta) (UnconsolidatedBlock, error)
}

// SeriesMeta is metadata data for the series.
type SeriesMeta struct {
	Tags models.Tags
	Name string
}

// Iterator is the base iterator.
type Iterator interface {
	// Next moves to the next item in the iterator. It will return false if there
	// are no more items, or if encountering an error.
	//
	// NB: it is important to check that Err() is nil after Next returns false, to
	// ensure that any errors during iteration are detected and accounted for.
	Next() bool
	// Err returns any error encountered during iteration.
	Err() error
	// Close frees up resources held by the iterator.
	Close()
}

// MetaIter is implemented by iterators which provide meta information.
type MetaIter interface {
	// SeriesMeta returns the metadata for each series in the block.
	SeriesMeta() []SeriesMeta
	// Meta returns the metadata for the block.
	Meta() Metadata
}

// SeriesMetaIter is implemented by series iterators which provide meta information.
type SeriesMetaIter interface {
	MetaIter
	// SeriesCount returns the number of series.
	SeriesCount() int
}

// SeriesIter iterates through a block horizontally.
type SeriesIter interface {
	Iterator
	SeriesMetaIter
	// Current returns the current series for the block.
	Current() Series
}

// UnconsolidatedSeriesIter iterates through a block horizontally.
type UnconsolidatedSeriesIter interface {
	Iterator
	SeriesMetaIter
	// Current returns the current series for the block.
	Current() UnconsolidatedSeries
}

// StepMetaIter is implemented by step iterators which provide meta information.
type StepMetaIter interface {
	MetaIter
	// StepCount returns the number of steps.
	StepCount() int
}

// StepIter iterates through a block vertically.
type StepIter interface {
	Iterator
	StepMetaIter
	// Current returns the current step for the block.
	Current() Step
}

// UnconsolidatedStepIter iterates through a block vertically.
type UnconsolidatedStepIter interface {
	Iterator
	StepMetaIter
	// Current returns the current step for the block.
	Current() UnconsolidatedStep
}

// Step is a single time step within a block.
type Step interface {
	Time() time.Time
	Values() []float64
}

// UnconsolidatedStep is a single unconsolidated time step within a block.
type UnconsolidatedStep interface {
	Time() time.Time
	Values() []ts.Datapoints
}

// Metadata is metadata for a block.
type Metadata struct {
	Bounds models.Bounds
	Tags   models.Tags // Common tags across different series
}

// String returns a string representation of metadata.
func (m Metadata) String() string {
	return fmt.Sprintf("Bounds: %v, Tags: %v", m.Bounds, m.Tags)
}

// Builder builds a new block.
type Builder interface {
	AppendValue(idx int, value float64) error
	AppendValues(idx int, values []float64) error
	Build() Block
	AddCols(num int) error
}

// Result is the result from a block query.
type Result struct {
	Blocks []Block
}

// ConsolidationFunc consolidates a bunch of datapoints into a single float value.
type ConsolidationFunc func(datapoints ts.Datapoints) float64

// TakeLast is a consolidation function which takes the last datapoint which has non nan value.
func TakeLast(values ts.Datapoints) float64 {
	for i := len(values) - 1; i >= 0; i-- {
		if !math.IsNaN(values[i].Value) {
			return values[i].Value
		}
	}

	return math.NaN()
}
