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

// Package m3 describes m3 metric id information.
//
// Each M3 metric id contains a metric name and a set of tag pairs. In particular,
// it conforms to the following format:
//
// m3+<metric_name>+tagName1=tagValue1,tagName2=tagValue2,...tagNameN=tagValueN,
//
// Where the tag names are sorted alphabetically in ascending order.
//
// An example m3 metrid id is as follows:
// m3+response_code+env=bar,service=foo,status=404,type=counters
package m3

import (
	"bytes"
	"errors"
	"sort"

	"github.com/m3db/m3/src/metrics/metric/id"
)

const (
	componentSplitter = '+'
	tagNameSplitter   = '='
	tagPairSplitter   = ','
)

var (
	errInvalidM3Metric = errors.New("invalid m3 metric")
	m3Prefix           = []byte("m3+")
	rollupTagPair      = id.TagPair{
		Name:  []byte("m3_rollup"),
		Value: []byte("true"),
	}
)

type rollupIDer struct{}

// NewRollupIDer creates a new IDer for rollups.
func NewRollupIDer() id.IDer {
	return &rollupIDer{}
}

// ID generates a new rollup id given the new metric name
// and a list of tag pairs. Note that tagPairs are mutated in place.
func (r *rollupIDer) ID(name []byte, tagPairs []id.TagPair) ([]byte, error) {
	var buf bytes.Buffer

	// Adding rollup tag pair to the list of tag pairs.
	tagPairs = append(tagPairs, rollupTagPair)
	sort.Sort(id.TagPairsByNameAsc(tagPairs))

	buf.Write(m3Prefix)
	buf.Write(name)
	buf.WriteByte(componentSplitter)
	for i, p := range tagPairs {
		buf.Write(p.Name)
		buf.WriteByte(tagNameSplitter)
		buf.Write(p.Value)
		if i < len(tagPairs)-1 {
			buf.WriteByte(tagPairSplitter)
		}
	}

	return buf.Bytes(), nil
}

// IsRollupID determines whether an id is a rollup id.
// Caller may optionally pass in a sorted tag iterator
// pool for efficiency reasons.
// nolint: unparam
func IsRollupID(name []byte, tags []byte, iterPool id.SortedTagIteratorPool) bool {
	var iter id.SortedTagIterator
	if iterPool == nil {
		iter = NewSortedTagIterator(tags)
	} else {
		iter = iterPool.Get()
		iter.Reset(tags)
	}
	defer iter.Close()

	for iter.Next() {
		name, val := iter.Current()
		if bytes.Equal(name, rollupTagPair.Name) && bytes.Equal(val, rollupTagPair.Value) {
			return true
		}
	}
	return false
}

// TODO(xichen): pool the mids.
type metricID struct {
	iterPool id.SortedTagIteratorPool
	id       []byte
}

// NewID creates a new m3 metric id.
func NewID(id []byte, iterPool id.SortedTagIteratorPool) id.ID {
	return metricID{id: id, iterPool: iterPool}
}

func (id metricID) Bytes() []byte { return id.id }

func (id metricID) TagValue(tagName []byte) ([]byte, bool) {
	_, tagPairs, err := NameAndTags(id.Bytes())
	if err != nil {
		return nil, false
	}

	it := id.iterPool.Get()
	it.Reset(tagPairs)
	defer it.Close()

	for it.Next() {
		n, v := it.Current()
		if bytes.Equal(tagName, n) {
			return v, true
		}
	}
	return nil, false
}

// NameAndTags returns the name and the tags of the given id.
func NameAndTags(id []byte) ([]byte, []byte, error) {
	firstSplitterIdx := bytes.IndexByte(id, componentSplitter)
	if !bytes.HasSuffix(id[:firstSplitterIdx+1], m3Prefix) {
		return nil, nil, errInvalidM3Metric
	}
	secondSplitterIdx := bytes.IndexByte(id[firstSplitterIdx+1:], componentSplitter)
	if secondSplitterIdx == -1 {
		return nil, nil, errInvalidM3Metric
	}
	secondSplitterIdx = firstSplitterIdx + 1 + secondSplitterIdx
	name := id[firstSplitterIdx+1 : secondSplitterIdx]
	tags := id[secondSplitterIdx+1:]
	return name, tags, nil
}

type sortedTagIterator struct {
	err            error
	pool           id.SortedTagIteratorPool
	sortedTagPairs []byte
	tagName        []byte
	tagValue       []byte
	idx            int
}

// NewSortedTagIterator creates a new sorted tag iterator.
func NewSortedTagIterator(sortedTagPairs []byte) id.SortedTagIterator {
	return NewPooledSortedTagIterator(sortedTagPairs, nil)
}

// NewPooledSortedTagIterator creates a new pooled sorted tag iterator.
func NewPooledSortedTagIterator(sortedTagPairs []byte, pool id.SortedTagIteratorPool) id.SortedTagIterator {
	it := &sortedTagIterator{pool: pool}
	it.Reset(sortedTagPairs)
	return it
}

func (it *sortedTagIterator) Reset(sortedTagPairs []byte) {
	it.sortedTagPairs = sortedTagPairs
	it.idx = 0
	it.tagName = nil
	it.tagValue = nil
	it.err = nil
}

func (it *sortedTagIterator) Next() bool {
	if it.err != nil || it.idx >= len(it.sortedTagPairs) {
		return false
	}
	nameSplitterIdx := bytes.IndexByte(it.sortedTagPairs[it.idx:], tagNameSplitter)
	if nameSplitterIdx == -1 {
		it.err = errInvalidM3Metric
		return false
	}
	nameSplitterIdx = it.idx + nameSplitterIdx
	pairSplitterIdx := bytes.IndexByte(it.sortedTagPairs[nameSplitterIdx+1:], tagPairSplitter)
	if pairSplitterIdx != -1 {
		pairSplitterIdx = nameSplitterIdx + 1 + pairSplitterIdx
	} else {
		pairSplitterIdx = len(it.sortedTagPairs)
	}
	it.tagName = it.sortedTagPairs[it.idx:nameSplitterIdx]
	it.tagValue = it.sortedTagPairs[nameSplitterIdx+1 : pairSplitterIdx]
	it.idx = pairSplitterIdx + 1
	return true
}

func (it *sortedTagIterator) Current() ([]byte, []byte) {
	return it.tagName, it.tagValue
}

func (it *sortedTagIterator) Err() error {
	return it.err
}

func (it *sortedTagIterator) Close() {
	it.sortedTagPairs = nil
	it.idx = 0
	it.tagName = nil
	it.tagValue = nil
	it.err = nil
	if it.pool != nil {
		it.pool.Put(it)
	}
}
