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

package multiresults

import (
	"bytes"
	"errors"
	"sort"

	"github.com/m3db/m3/src/query/storage"
)

type completeTagsResultBuilder struct {
	nameOnly    bool
	tagBuilders *multiCompleteTagsMap
}

// NewCompleteTagsResultBuilder creates a new complete tags result builder.
func NewCompleteTagsResultBuilder(
	nameOnly bool,
) CompleteTagsResultBuilder {
	return &completeTagsResultBuilder{
		nameOnly: nameOnly,
	}
}

func (b *completeTagsResultBuilder) Add(tagResult *storage.CompleteTagsResult) error {
	nameOnly := b.nameOnly
	if nameOnly != tagResult.CompleteNameOnly {
		return errors.New("incoming tag result has mismatched type")
	}

	completedTags := tagResult.CompletedTags
	if b.tagBuilders == nil {
		b.tagBuilders = newMultiCompleteTagsMap(multiCompleteTagsMapOptions{
			InitialSize: len(completedTags),
		})
	}

	if nameOnly {
		for _, tag := range completedTags {
			// Set unsafe since the id put into this map is much shorter
			// lived than the ids in the object added to it;
			// Copying or finalizing would use unnecessary memory
			opts := multiCompleteTagsMapSetUnsafeOptions{
				NoCopyKey:     true,
				NoFinalizeKey: true,
			}

			b.tagBuilders.SetUnsafe(
				tag.Name,
				completedTagBuilder{},
				opts,
			)
		}

		return nil
	}

	for _, tag := range completedTags {
		name := tag.Name
		// Set unsafe since the id put into this map is much shorter
		// lived than the ids in the object added to it;
		// Copying or finalizing would use unnecessary memory
		opts := multiCompleteTagsMapSetUnsafeOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		}

		if builder, exists := b.tagBuilders.Get(name); exists {
			builder.add(tag.Values)
			b.tagBuilders.SetUnsafe(
				name,
				builder,
				opts,
			)
		} else {
			builder := completedTagBuilder{}
			builder.add(tag.Values)
			b.tagBuilders.SetUnsafe(
				name,
				builder,
				opts,
			)
		}
	}

	return nil
}

type completedTagsByName []storage.CompletedTag

func (s completedTagsByName) Len() int      { return len(s) }
func (s completedTagsByName) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s completedTagsByName) Less(i, j int) bool {
	return bytes.Compare(s[i].Name, s[j].Name) == -1
}

func (b *completeTagsResultBuilder) Build() storage.CompleteTagsResult {
	if b.tagBuilders == nil {
		return storage.CompleteTagsResult{
			CompleteNameOnly: b.nameOnly,
			CompletedTags:    []storage.CompletedTag(nil),
		}
	}

	result := make([]storage.CompletedTag, 0, b.tagBuilders.Len())
	if b.nameOnly {
		for _, entry := range b.tagBuilders.Iter() {
			result = append(result, storage.CompletedTag{
				Name:   entry.Key(),
				Values: [][]byte{},
			})
		}

		sort.Sort(completedTagsByName(result))
		return storage.CompleteTagsResult{
			CompleteNameOnly: true,
			CompletedTags:    result,
		}
	}

	for _, entry := range b.tagBuilders.Iter() {
		builder := entry.Value()
		result = append(result, storage.CompletedTag{
			Name:   entry.Key(),
			Values: builder.build(),
		})
	}

	sort.Sort(completedTagsByName(result))
	return storage.CompleteTagsResult{
		CompleteNameOnly: false,
		CompletedTags:    result,
	}
}

type completedTagBuilder struct {
	seenMap *multiCompleteTagValuesMap
}

func (b *completedTagBuilder) add(values [][]byte) {
	if b.seenMap == nil {
		b.seenMap = newMultiCompleteTagValuesMap(multiCompleteTagValuesMapOptions{
			InitialSize: len(values),
		})
	}

	for _, val := range values {
		// Set unsafe since the id put into this map is much shorter
		// lived than the ids in the object added to it;
		// Copying or finalizing would use unnecessary memory
		b.seenMap.SetUnsafe(
			val,
			seen{},
			multiCompleteTagValuesMapSetUnsafeOptions{
				NoCopyKey:     true,
				NoFinalizeKey: true,
			})
	}
}

type tagValuesByName [][]byte

func (s tagValuesByName) Len() int      { return len(s) }
func (s tagValuesByName) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s tagValuesByName) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) == -1
}

func (b *completedTagBuilder) build() [][]byte {
	if b.seenMap == nil {
		return [][]byte{}
	}

	result := make([][]byte, 0, b.seenMap.Len())
	for _, seen := range b.seenMap.Iter() {
		result = append(result, seen.Key())
	}

	sort.Sort(tagValuesByName(result))
	return result
}
