// Copyright (c) 2017 Uber Technologies, Inc.
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

package series

import (
	"errors"
	"fmt"
)

var (
	errCachePolicyUnspecified = errors.New("series cache policy unspecified")
)

// CachePolicy is the series cache policy.
type CachePolicy uint

const (
	// CacheAll specifies that all series must be cached at all times
	// which requires loading all into cache on bootstrap and never
	// expiring series from memory until expired from retention.
	CacheAll CachePolicy = iota
	// CacheAllMetadata specifies that all series metadata but not the
	// data itself must be cached at all times and the metadata is never
	// expired from memory until expired from retention.
	// TODO: Remove this once recently read is production grade.
	CacheAllMetadata
	// CacheRecentlyRead specifies that series that are recently read
	// must be cached, configurable by the namespace block expiry after
	// not accessed period.
	CacheRecentlyRead

	// DefaultCachePolicy is the default cache policy.
	DefaultCachePolicy = CacheRecentlyRead
)

// ValidCachePolicies returns the valid series cache policies.
func ValidCachePolicies() []CachePolicy {
	return []CachePolicy{CacheAll, CacheAllMetadata, CacheRecentlyRead}
}

func (p CachePolicy) String() string {
	switch p {
	case CacheAll:
		return "all"
	case CacheAllMetadata:
		return "all_metadata"
	case CacheRecentlyRead:
		return "recently_read"
	}
	return "unknown"
}

// ValidateCachePolicy validates a cache policy.
func ValidateCachePolicy(v CachePolicy) error {
	validSeriesCachePolicy := false
	for _, valid := range ValidCachePolicies() {
		if valid == v {
			validSeriesCachePolicy = true
			break
		}
	}
	if !validSeriesCachePolicy {
		return fmt.Errorf("invalid series CachePolicy '%d' valid types are: %v",
			uint(v), ValidCachePolicies())
	}
	return nil
}

// UnmarshalYAML unmarshals an CachePolicy into a valid type from string.
func (p *CachePolicy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return errCachePolicyUnspecified
	}
	for _, valid := range ValidCachePolicies() {
		if str == valid.String() {
			*p = valid
			return nil
		}
	}
	return fmt.Errorf("invalid series CachePolicy '%s' valid types are: %v",
		str, ValidCachePolicies())
}
