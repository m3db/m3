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

package config

import "github.com/m3db/m3/src/dbnode/storage/series"

var (
	defaultPostingsListCacheSize   = 2 << 11 // 4096
	defaultPostingsListCacheRegexp = true
	defaultPostingsListCacheTerms  = true
	defaultPostingsListCacheSearch = true
	defaultRegexpCacheSize         = 256
)

// CacheConfigurations is the cache configurations.
type CacheConfigurations struct {
	// Series cache policy.
	Series *SeriesCacheConfiguration `yaml:"series"`

	// PostingsList cache policy.
	PostingsList *PostingsListCacheConfiguration `yaml:"postingsList"`

	// Regexp cache policy.
	Regexp *RegexpCacheConfiguration `yaml:"regexp"`
}

// SeriesConfiguration returns the series cache configuration or default
// if none is specified.
func (c CacheConfigurations) SeriesConfiguration() SeriesCacheConfiguration {
	if c.Series == nil {
		// Return default cache configuration
		return SeriesCacheConfiguration{Policy: series.DefaultCachePolicy}
	}
	return *c.Series
}

// PostingsListConfiguration returns the postings list cache configuration
// or default if none is specified.
func (c CacheConfigurations) PostingsListConfiguration() PostingsListCacheConfiguration {
	if c.PostingsList == nil {
		return PostingsListCacheConfiguration{}
	}
	return *c.PostingsList
}

// RegexpConfiguration returns the regexp cache configuration or default
// if none is specified.
func (c CacheConfigurations) RegexpConfiguration() RegexpCacheConfiguration {
	if c.Regexp == nil {
		return RegexpCacheConfiguration{}
	}
	return *c.Regexp
}

// SeriesCacheConfiguration is the series cache configuration.
type SeriesCacheConfiguration struct {
	Policy series.CachePolicy                 `yaml:"policy"`
	LRU    *LRUSeriesCachePolicyConfiguration `yaml:"lru"`
}

// LRUSeriesCachePolicyConfiguration contains configuration for the LRU
// series caching policy.
type LRUSeriesCachePolicyConfiguration struct {
	MaxBlocks         uint `yaml:"maxBlocks" validate:"nonzero"`
	EventsChannelSize uint `yaml:"eventsChannelSize" validate:"nonzero"`
}

// PostingsListCacheConfiguration is the postings list cache configuration.
type PostingsListCacheConfiguration struct {
	Size        *int  `yaml:"size"`
	CacheRegexp *bool `yaml:"cacheRegexp"`
	CacheTerms  *bool `yaml:"cacheTerms"`
	CacheSearch *bool `yaml:"cacheSearch"`
}

// SizeOrDefault returns the provided size or the default value is none is
// provided.
func (p PostingsListCacheConfiguration) SizeOrDefault() int {
	if p.Size == nil {
		return defaultPostingsListCacheSize
	}

	return *p.Size
}

// CacheRegexpOrDefault returns the provided cache regexp configuration value
// or the default value is none is provided.
func (p PostingsListCacheConfiguration) CacheRegexpOrDefault() bool {
	if p.CacheRegexp == nil {
		return defaultPostingsListCacheRegexp
	}

	return *p.CacheRegexp
}

// CacheTermsOrDefault returns the provided cache terms configuration value
// or the default value is none is provided.
func (p PostingsListCacheConfiguration) CacheTermsOrDefault() bool {
	if p.CacheTerms == nil {
		return defaultPostingsListCacheTerms
	}

	return *p.CacheTerms
}

// CacheSearchOrDefault returns the provided cache search configuration value
// or the default value is none is provided.
func (p PostingsListCacheConfiguration) CacheSearchOrDefault() bool {
	if p.CacheSearch == nil {
		return defaultPostingsListCacheSearch
	}

	return *p.CacheSearch
}

// RegexpCacheConfiguration is a compiled regexp cache for query regexps.
type RegexpCacheConfiguration struct {
	Size *int `yaml:"size"`
}

// SizeOrDefault returns the provided size or the default value is none is
// provided.
func (c RegexpCacheConfiguration) SizeOrDefault() int {
	if c.Size == nil {
		return defaultRegexpCacheSize
	}

	return *c.Size
}
