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
	"sort"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/models"
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

// ResultMetricMetadata describes metadata on a per metric-name basis.
type ResultMetricMetadata struct {
	// NoSamples is the total number of series that were fetched to compute
	// this result but had no samples.
	NoSamples int
	// WithSamples is the total number of series that were fetched to compute
	// this result and had samples.
	WithSamples int
	// Aggregated is the total number of aggregated series that were fetched to
	// compute this result.
	Aggregated int
	// Unaggregated is the total number of unaggregated series that were fetched to
	// compute this result.
	Unaggregated int
}

// Equals determines if two result metric metadatas are equal.
func (m ResultMetricMetadata) Equals(other ResultMetricMetadata) bool {
	if m.NoSamples != other.NoSamples {
		return false
	}
	if m.WithSamples != other.WithSamples {
		return false
	}
	if m.Aggregated != other.Aggregated {
		return false
	}
	if m.Unaggregated != other.Unaggregated {
		return false
	}
	return true
}

// Merge takes another ResultMetricMetadata and merges it into this one.
func (m *ResultMetricMetadata) Merge(other ResultMetricMetadata) {
	m.NoSamples += other.NoSamples
	m.WithSamples += other.WithSamples
	m.Aggregated += other.Aggregated
	m.Unaggregated += other.Unaggregated
}

func mergeMetricMetadataMaps(dst, src map[string]*ResultMetricMetadata) {
	for name, other := range src {
		m, ok := dst[name]
		if !ok {
			dst[name] = other
		} else {
			m.Merge(*other)
		}
	}
}

// ResultMetadata describes metadata common to each type of query results,
// indicating any additional information about the result.
type ResultMetadata struct {
	// Namespaces are the set of namespaces queried.
	// External users must access via `AddNamespace`
	namespaces map[string]struct{}
	// FetchedResponses is the number of M3 RPC fetch responses received.
	FetchedResponses int
	// FetchedBytesEstimate is the estimated number of bytes fetched.
	FetchedBytesEstimate int
	// LocalOnly indicates that this query was executed only on the local store.
	LocalOnly bool
	// Exhaustive indicates whether the underlying data set presents a full
	// collection of retrieved data.
	Exhaustive bool
	// Warnings is a list of warnings that indicate potentially partial or
	// incomplete results.
	Warnings Warnings
	// Resolutions is a list of resolutions for series obtained by this query.
	Resolutions []time.Duration
	// KeepNaNs indicates if NaNs should be kept when returning results.
	KeepNaNs bool
	// WaitedIndex counts how many times index querying had to wait for permits.
	WaitedIndex int
	// WaitedSeriesRead counts how many times series being read had to wait for permits.
	WaitedSeriesRead int
	// FetchedSeriesCount is the total number of series that were fetched to compute
	// this result.
	FetchedSeriesCount int
	// FetchedMetadataCount is the total amount of metadata that was fetched to compute
	// this result.
	FetchedMetadataCount int
	// MetricNames is the set of unique metric tag name values across all series in this result.
	// External users must access via `ByName(name)`.
	metadataByName map[string]*ResultMetricMetadata
}

// AddNamespace adds a namespace to the namespace set, initializing the underlying map if necessary.
func (m *ResultMetadata) AddNamespace(namespace string) {
	if m.namespaces == nil {
		m.namespaces = make(map[string]struct{})
	}
	m.namespaces[namespace] = struct{}{}
}

// GetNamespaces returns an array representing the set of namespaces added via AddNamespace.
func (m ResultMetadata) GetNamespaces() []string {
	if m.namespaces == nil {
		return []string{}
	}
	namespaces := []string{}
	for n := range m.namespaces {
		namespaces = append(namespaces, n)
	}
	sort.Strings(namespaces)
	return namespaces
}

// ByName returns the ResultMetricMetadata for a given metric name.
func (m *ResultMetadata) ByName(nameTag []byte) *ResultMetricMetadata {
	if m.metadataByName == nil {
		m.metadataByName = make(map[string]*ResultMetricMetadata)
	}

	r, ok := m.metadataByName[string(nameTag)]
	if ok {
		return r
	}

	r = &ResultMetricMetadata{}
	m.metadataByName[string(nameTag)] = r
	return r
}

// MetadataByNameMerged returns the metadataByName map values merged into one.
func (m ResultMetadata) MetadataByNameMerged() ResultMetricMetadata {
	r := ResultMetricMetadata{}
	for _, m := range m.metadataByName {
		r.Merge(*m)
	}
	return r
}

// TopMetadataByName returns the top `max` ResultMetricMetadatas by the sum of their
// contained counters.
func (m ResultMetadata) TopMetadataByName(max int) map[string]*ResultMetricMetadata {
	if len(m.metadataByName) <= max {
		return m.metadataByName
	}

	keys := []string{}
	for k := range m.metadataByName {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		a := m.metadataByName[keys[i]]
		b := m.metadataByName[keys[j]]
		n := a.Aggregated + a.Unaggregated + a.NoSamples + a.WithSamples
		m := b.Aggregated + b.Unaggregated + b.NoSamples + b.WithSamples
		// Sort in descending order
		return n > m
	})
	top := make(map[string]*ResultMetricMetadata, max)
	for i := 0; i < max; i++ {
		k := keys[i]
		top[k] = m.metadataByName[k]
	}
	return top
}

// NewResultMetadata creates a new result metadata.
func NewResultMetadata() ResultMetadata {
	return ResultMetadata{
		LocalOnly:  true,
		Exhaustive: true,
	}
}

func combineResolutions(a, b []time.Duration) []time.Duration {
	if len(a) == 0 {
		if len(b) != 0 {
			return b
		}
	} else {
		if len(b) == 0 {
			return a
		}

		combined := make([]time.Duration, 0, len(a)+len(b))
		combined = append(combined, a...)
		combined = append(combined, b...)
		return combined
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
		}

		combinedWarnings := make(Warnings, 0, len(a)+len(b))
		combinedWarnings = append(combinedWarnings, a...)
		return combinedWarnings.addWarnings(b...)
	}

	return nil
}

func combineNamespaces(a, b map[string]struct{}) map[string]struct{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	merged := make(map[string]struct{})
	for n := range a {
		merged[n] = struct{}{}
	}
	for n := range b {
		merged[n] = struct{}{}
	}
	return merged
}

func combineMetricMetadata(a, b map[string]*ResultMetricMetadata) map[string]*ResultMetricMetadata {
	if a == nil && b == nil {
		return nil
	}

	merged := make(map[string]*ResultMetricMetadata)
	mergeMetricMetadataMaps(merged, a)
	mergeMetricMetadataMaps(merged, b)

	return merged
}

// Equals determines if two result metadatas are equal.
func (m ResultMetadata) Equals(n ResultMetadata) bool {
	if m.Exhaustive && !n.Exhaustive || !m.Exhaustive && n.Exhaustive {
		return false
	}

	if m.LocalOnly && !n.LocalOnly || !m.LocalOnly && n.LocalOnly {
		return false
	}

	if len(m.Resolutions) != len(n.Resolutions) {
		return false
	}

	for i, mRes := range m.Resolutions {
		if n.Resolutions[i] != mRes {
			return false
		}
	}

	for i, mWarn := range m.Warnings {
		if !n.Warnings[i].equals(mWarn) {
			return false
		}
	}

	if m.WaitedIndex != n.WaitedIndex {
		return false
	}

	if m.WaitedSeriesRead != n.WaitedSeriesRead {
		return false
	}

	if m.FetchedSeriesCount != n.FetchedSeriesCount {
		return false
	}

	if !m.MetadataByNameMerged().Equals(n.MetadataByNameMerged()) {
		return false
	}

	return m.FetchedMetadataCount == n.FetchedMetadataCount
}

// CombineMetadata combines two result metadatas.
func (m ResultMetadata) CombineMetadata(other ResultMetadata) ResultMetadata {
	return ResultMetadata{
		namespaces:           combineNamespaces(m.namespaces, other.namespaces),
		FetchedResponses:     m.FetchedResponses + other.FetchedResponses,
		FetchedBytesEstimate: m.FetchedBytesEstimate + other.FetchedBytesEstimate,
		LocalOnly:            m.LocalOnly && other.LocalOnly,
		Exhaustive:           m.Exhaustive && other.Exhaustive,
		Warnings:             combineWarnings(m.Warnings, other.Warnings),
		Resolutions:          combineResolutions(m.Resolutions, other.Resolutions),
		WaitedIndex:          m.WaitedIndex + other.WaitedIndex,
		WaitedSeriesRead:     m.WaitedSeriesRead + other.WaitedSeriesRead,
		FetchedSeriesCount:   m.FetchedSeriesCount + other.FetchedSeriesCount,
		metadataByName:       combineMetricMetadata(m.metadataByName, other.metadataByName),
		FetchedMetadataCount: m.FetchedMetadataCount + other.FetchedMetadataCount,
	}
}

// IsDefault returns true if this result metadata matches the unchanged default.
func (m ResultMetadata) IsDefault() bool {
	return m.Exhaustive && m.LocalOnly && len(m.Warnings) == 0
}

// VerifyTemporalRange will verify that each resolution seen is below the
// given step size, adding warning headers if it is not.
func (m *ResultMetadata) VerifyTemporalRange(step time.Duration) {
	// NB: this map is unlikely to have more than 2 elements in real execution,
	// since these correspond to namespace count.
	invalidResolutions := make(map[time.Duration]struct{}, 10)
	for _, res := range m.Resolutions {
		if res > step {
			invalidResolutions[res] = struct{}{}
		}
	}

	if len(invalidResolutions) > 0 {
		warnings := make([]string, 0, len(invalidResolutions))
		for k := range invalidResolutions {
			warnings = append(warnings, fmt.Sprintf("%v", k))
		}

		sort.Strings(warnings)
		warning := fmt.Sprintf("range: %v, resolutions: %s",
			step, strings.Join(warnings, ", "))
		m.AddWarning("resolution larger than query range", warning)
	}
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

// AddWarnings adds several warnings to the result metadata.
func (m *ResultMetadata) AddWarnings(warnings ...Warning) {
	m.Warnings = m.Warnings.addWarnings(warnings...)
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

// WarningStrings converts warnings to a slice of strings for presentation.
func (m ResultMetadata) WarningStrings() []string {
	size := len(m.Warnings)
	if !m.Exhaustive {
		size++
	}

	strs := make([]string, 0, size)
	for _, warn := range m.Warnings {
		strs = append(strs, warn.Header())
	}

	if !m.Exhaustive {
		strs = append(strs, "m3db exceeded query limit: results not exhaustive")
	}

	return strs
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
