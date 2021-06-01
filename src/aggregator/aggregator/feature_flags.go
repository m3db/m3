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
	"encoding/binary"
)

// FeatureFlagConfigurations is a list of aggregator feature flags.
type FeatureFlagConfigurations []FeatureFlagConfiguration

// Parse converts FeatureFlagConfigurations into a list of
// FeatureFlagBundleParsed. The difference being, tag (key, value) pairs are
// represented as []byte in the FeatureFlagBundleParsed. The bytes are used to
// match against metric ids for applying feature flags.
func (f FeatureFlagConfigurations) Parse() []FeatureFlagBundleParsed {
	result := make([]FeatureFlagBundleParsed, 0, len(f))
	for i, elem := range f {
		result[i] = elem.parse()
	}
	return result
}

// FeatureFlagConfiguration holds filter and flag combinations. The flags are
// scoped to metrics with tags that match the filter.
type FeatureFlagConfiguration struct {
	// Filter is a map of tag keys and values that much match for the flags to
	// be applied.
	Filter map[string]string `yaml:"filter"`
	// Flags are the flags enabled once the filters are matched.
	Flags FlagBundle `yaml:"flags"`
}

// FlagBundle contains all aggregator feature flags.
type FlagBundle struct {
	// IncreaseWithPrevNaNTranslatesToCurrValueIncrease configures the binary
	// increase operation to fill in prev NaN values with 0.
	IncreaseWithPrevNaNTranslatesToCurrValueIncrease bool `yaml:"increaseWithPrevNaNTranslatesToCurrValueIncrease"`
}

func (f FeatureFlagConfiguration) parse() FeatureFlagBundleParsed {
	parsed := FeatureFlagBundleParsed{
		flags:                 f.Flags,
		serializedTagMatchers: make([][]byte, 0, len(f.Filter)),
	}

	i := 0
	for key, value := range f.Filter {
		var (
			byteOrder      = binary.LittleEndian
			buff           = make([]byte, 2)
			tagFilterBytes []byte
		)

		// Add key bytes.
		byteOrder.PutUint16(buff[:2], uint16(len(key)))
		tagFilterBytes = append(tagFilterBytes, buff[:2]...)
		tagFilterBytes = append(tagFilterBytes, []byte(key)...)

		// Add value bytes.
		byteOrder.PutUint16(buff[:2], uint16(len(value)))
		tagFilterBytes = append(tagFilterBytes, buff[:2]...)
		tagFilterBytes = append(tagFilterBytes, []byte(value)...)

		parsed.serializedTagMatchers[i] = tagFilterBytes
		i++
	}

	return parsed
}

// FeatureFlagBundleParsed is a parsed feature flag bundle.
type FeatureFlagBundleParsed struct {
	flags                 FlagBundle
	serializedTagMatchers [][]byte
}

// Match matches the given byte string with all filters for the
// parsed feature flag bundle.
func (f FeatureFlagBundleParsed) Match(metricID []byte) (FlagBundle, bool) {
	for _, val := range f.serializedTagMatchers {
		if !bytes.Contains(metricID, val) {
			return FlagBundle{}, false
		}
	}
	return f.flags, true
}
