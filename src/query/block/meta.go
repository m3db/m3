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

package block

import (
	"fmt"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
)

// Metadata is metadata for a block, describing size and common tags across
// constituent series.
type Metadata struct {
	// Bounds represents the time bounds for all series in the block.
	Bounds models.Bounds
	// Tags contains any tags common across all series in the block.
	Tags models.Tags
	// ResultMetadata contains metadata from any database access operations during
	// fetching block details.
	ResultMetadata ResultMetadata
}

// Equals returns a boolean reporting whether the compared metadata has equal
// fields.
func (m Metadata) Equals(other Metadata) bool {
	return m.Tags.Equals(other.Tags) && m.Bounds.Equals(other.Bounds)
}

// String returns a string representation of metadata.
func (m Metadata) String() string {
	return fmt.Sprintf("Bounds: %v, Tags: %v", m.Bounds, m.Tags)
}

// Warnings is a slice of warnings.
type Warnings []Warning

// ResultMetadata describes metadata common to each type of query results,
// indicating any additional information about the result.
type ResultMetadata struct {
	// LocalOnly indicates that this query was executed only on the local store.
	LocalOnly bool
	// Exhaustive indicates whether the underlying data set presents a full
	// collection of retrieved data.
	Exhaustive bool
	// Warnings is a list of warnings that indicate potetitally partial or
	// incomplete results.
	Warnings Warnings
	// Resolutions is a list of resolutions for series obtained by this query.
	Resolutions []int64
	// ExemplarsList is a list of exemplars per datapoint per series obtained
	// by this query.
	ExemplarsList ts.SeriesExemplarList
}

// NewResultMetadata creates a new result metadata.
func NewResultMetadata() ResultMetadata {
	return ResultMetadata{
		LocalOnly:  true,
		Exhaustive: true,
	}
}

func combineResolutions(a, b []int64) []int64 {
	if len(a) == 0 {
		if len(b) != 0 {
			return b
		}
	} else {
		if len(b) == 0 {
			return a
		} else {
			combined := make([]int64, 0, len(a)+len(b))
			combined = append(combined, a...)
			combined = append(combined, b...)
			return combined
		}
	}

	return nil
}

func combineWarnings(a, b Warnings) Warnings {
	if len(a) == 0 {
		if len(b) != 0 {
			return b
		}
	} else {
		if len(b) == 0 {
			return a
		} else {
			combinedWarnings := make(Warnings, 0, len(a)+len(b))
			combinedWarnings = append(combinedWarnings, a...)
			return combinedWarnings.addWarnings(b...)
		}
	}

	return nil
}

func combineExemplarsList(a, b ts.SeriesExemplarList) ts.SeriesExemplarList {
	if len(a) == 0 {
		if len(b) != 0 {
			return b
		}
	} else {
		if len(b) == 0 {
			return a
		} else {
			combined := make(ts.SeriesExemplarList, 0, len(a)+len(b))
			combined = append(combined, a...)
			combined = append(combined, b...)
			return combined
		}
	}

	return nil
}

// CombineMetadata combines two result metadatas.
func (m ResultMetadata) CombineMetadata(other ResultMetadata) ResultMetadata {
	meta := ResultMetadata{
		LocalOnly:     m.LocalOnly && other.LocalOnly,
		Exhaustive:    m.Exhaustive && other.Exhaustive,
		Warnings:      combineWarnings(m.Warnings, other.Warnings),
		Resolutions:   combineResolutions(m.Resolutions, other.Resolutions),
		ExemplarsList: combineExemplarsList(m.ExemplarsList, other.ExemplarsList),
	}

	return meta
}

// IsDefault returns true if this result metadata matches the unchanged default.
func (m ResultMetadata) IsDefault() bool {
	return m.Exhaustive && m.LocalOnly && len(m.Warnings) == 0
}

// AddWarning adds a warning to the result metadata.
// NB: warnings are expected to be small in general, so it's better to iterate
// over the array rather than introduce a map.
func (m *ResultMetadata) AddWarning(name string, message string) {
	m.Warnings = m.Warnings.addWarnings(Warning{
		Name:    name,
		Message: message,
	})
}

// NB: this is not a very efficient merge but this is extremely unlikely to be
// merging more than 5 or 6 total warnings.
func (w Warnings) addWarnings(warnings ...Warning) Warnings {
	for _, newWarning := range warnings {
		found := false
		for _, warning := range w {
			if warning.equals(newWarning) {
				found = true
				break
			}
		}

		if !found {
			w = append(w, newWarning)
		}
	}

	return w
}

// Warning is a message that indicates potential partial or incomplete results.
type Warning struct {
	// Name is the name of the store originating the warning.
	Name string
	// Message is the content of the warning message.
	Message string
}

// Header formats the warning into a format to send in a response header.
func (w Warning) Header() string {
	return fmt.Sprintf("%s_%s", w.Name, w.Message)
}

func (w Warning) equals(warning Warning) bool {
	return w.Name == warning.Name && w.Message == warning.Message
}
