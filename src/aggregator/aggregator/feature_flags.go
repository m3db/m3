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

package aggregator

import (
	"bytes"
)

// FeatureFlagConfiguration is a list of aggregator feature flags.
type FeatureFlagConfiguration []FlagConfiguration

// FlagConfiguration holds filter and flag combinations. The flags are scoped
// to metrics with tags that match the filter.
type FlagConfiguration struct {
	// Filter is a map of tag keys and values that much match for the flags to
	// be applied.
	Filter map[string]string `yaml:"filter"`
	// Flags are the flags enabled once the filters are matched.
	Flags FlagBundle `yaml:"flags"`

	// filterMultiBytes contains the encoded filter tag keys and values.
	filterMultiBytes [][]byte
}

// FlagBundle contains all aggregator feature flags.
type FlagBundle struct {
	// IncreaseWithPrevNaNTranslatesToCurrValueIncrease configures the binary
	// increase operation to fill in prev NaN values with 0.
	IncreaseWithPrevNaNTranslatesToCurrValueIncrease bool `yaml:"increase_with_prev_nan_translates_to_curr_value_increase"`
}

// FilterMultiBytes returns the private filterMultiBytes field.
func (f *FlagConfiguration) FilterMultiBytes() [][]byte {
	return f.filterMultiBytes
}

// IsMatch matches the given byte string with all filters for the
// FlagConfiguration.
func (f *FlagConfiguration) IsMatch(metricID []byte) bool {
	for _, val := range f.filterMultiBytes {
		if !bytes.Contains(metricID, val) {
			return false
		}
	}

	return true
}
