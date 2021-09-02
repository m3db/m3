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

package storage

import (
	"bytes"
	"fmt"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

// Validate will validate the restrict fetch options.
func (o *RestrictQueryOptions) Validate() error {
	if o.RestrictByType != nil {
		return o.RestrictByType.Validate()
	}
	if len(o.RestrictByTypes) > 0 {
		for _, r := range o.RestrictByTypes {
			if err := r.Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetRestrictByType provides the type restrictions if present; nil otherwise.
func (o *RestrictQueryOptions) GetRestrictByType() *RestrictByType {
	if o == nil {
		return nil
	}

	return o.RestrictByType
}

// GetRestrictByTypes provides the types restrictions if present; nil otherwise.
func (o *RestrictQueryOptions) GetRestrictByTypes() []*RestrictByType {
	if o == nil {
		return nil
	}

	return o.RestrictByTypes
}

// GetRestrictByTag provides the tag restrictions if present; nil otherwise.
func (o *RestrictQueryOptions) GetRestrictByTag() *RestrictByTag {
	if o == nil {
		return nil
	}

	return o.RestrictByTag
}

// GetMatchers provides the tag matchers by which results are restricted if
// present; nil otherwise.
func (o *RestrictByTag) GetMatchers() models.Matchers {
	if o == nil {
		return nil
	}

	return o.Restrict
}

// Validate will validate the restrict type restrictions.
func (o *RestrictByType) Validate() error {
	switch o.MetricsType {
	case storagemetadata.UnaggregatedMetricsType:
		if o.StoragePolicy != policy.EmptyStoragePolicy {
			return fmt.Errorf(
				"expected no storage policy for unaggregated metrics type, "+
					"instead got: %v", o.StoragePolicy.String())
		}
	case storagemetadata.AggregatedMetricsType:
		if v := o.StoragePolicy.Resolution().Window; v <= 0 {
			return fmt.Errorf(
				"expected positive resolution window, instead got: %v", v)
		}
		if v := o.StoragePolicy.Resolution().Precision; v <= 0 {
			return fmt.Errorf(
				"expected positive resolution precision, instead got: %v", v)
		}
		if v := o.StoragePolicy.Retention().Duration(); v <= 0 {
			return fmt.Errorf(
				"expected positive retention, instead got: %v", v)
		}
	default:
		return fmt.Errorf(
			"unknown metrics type: %v", o.MetricsType)
	}
	return nil
}

// GetFilterByNames returns the tag names to filter out of the response.
func (o *RestrictByTag) GetFilterByNames() [][]byte {
	if o == nil {
		return nil
	}

	if o.Strip != nil {
		return o.Strip
	}

	o.Strip = make([][]byte, 0, len(o.Restrict))
	for _, r := range o.Restrict {
		o.Strip = append(o.Strip, r.Name)
	}

	return o.Strip
}

// WithAppliedOptions returns a copy of the fetch query applied options
// that restricts the fetch with respect to labels that must be applied.
func (q *FetchQuery) WithAppliedOptions(
	opts *FetchOptions,
) *FetchQuery {
	result := *q
	if opts == nil {
		return &result
	}

	restrictOpts := opts.RestrictQueryOptions
	if restrictOpts == nil {
		return &result
	}

	restrict := restrictOpts.GetRestrictByTag().GetMatchers()
	if len(restrict) == 0 {
		return &result
	}

	// Since must apply matchers will always be small (usually 1)
	// it's better to not allocate intermediate datastructure and just
	// perform n^2 matching.
	existing := result.TagMatchers
	for _, existingMatcher := range result.TagMatchers {
		willBeOverridden := false
		for _, matcher := range restrict {
			if bytes.Equal(existingMatcher.Name, matcher.Name) {
				willBeOverridden = true
				break
			}
		}

		if willBeOverridden {
			// We'll override this when we append the restrict matchers.
			continue
		}

		existing = append(existing, existingMatcher)
	}

	// Now append the must apply matchers.
	result.TagMatchers = append(existing, restrict...)
	return &result
}

func (q *FetchQuery) String() string {
	return q.Raw
}
