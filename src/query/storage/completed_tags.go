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

package storage

import (
	"bytes"
	"errors"
	"sort"
)

// MergeWith merges two CompleteTagsResults together, handling deduplication.
// Once all merges are completed, Finalize() should be called to give sorted results.
func (r *CompleteTagsResult) MergeWith(other *CompleteTagsResult) error {
	nameOnly := r.CompleteNameOnly
	if nameOnly != other.CompleteNameOnly {
		return errors.New("cannot merge tag results with name only tag results")
	}

	if !r.merged {
		r.merged = true
		r.seenMap = make(map[string]int, len(r.CompletedTags))
		for i, tag := range r.CompletedTags {
			r.seenMap[string(tag.Name)] = i
		}
	}

	for _, otherTag := range other.CompletedTags {
		key := string(otherTag.Name)
		if idx, exists := r.seenMap[key]; exists {
			if nameOnly {
				continue
			}

			r.CompletedTags[idx].mergeWith(otherTag)
		} else {
			r.seenMap[key] = len(r.CompletedTags)
			r.CompletedTags = append(r.CompletedTags, otherTag)
		}
	}

	return nil
}

type alphabeticalTagSort []CompletedTag

func (s alphabeticalTagSort) Len() int      { return len(s) }
func (s alphabeticalTagSort) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s alphabeticalTagSort) Less(i, j int) bool {
	return bytes.Compare(s[i].Name, s[j].Name) == -1
}

// Finalize sorts the completed tags result in alphabetical order
// and releases the deduping map.
func (r *CompleteTagsResult) Finalize() {
	r.seenMap = nil
	r.merged = false
	for _, tag := range r.CompletedTags {
		tag.finalize()
	}

	sort.Sort(alphabeticalTagSort(r.CompletedTags))
}

func (t *CompletedTag) mergeWith(other CompletedTag) {
	if !t.merged {
		t.merged = true
		t.seenMap = make(map[string]struct{}, len(t.Values))
		for _, val := range t.Values {
			t.seenMap[string(val)] = struct{}{}
		}
	}

	for _, otherVal := range other.Values {
		key := string(otherVal)
		if _, exists := t.seenMap[key]; exists {
			continue
		}

		t.seenMap[key] = struct{}{}
		t.Values = append(t.Values, otherVal)
	}
}

type alphabeticalValueSort [][]byte

func (s alphabeticalValueSort) Len() int      { return len(s) }
func (s alphabeticalValueSort) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s alphabeticalValueSort) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) == -1
}

func (t *CompletedTag) finalize() {
	t.seenMap = nil
	t.merged = false

	sort.Sort(alphabeticalValueSort(t.Values))
}
