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

package filter

import (
	"strings"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
)
const (
	// NB: This is specific to Databricks!
	storageNameLabelKey = "shardName"
	localStorageName = "local_store"
)
// Storage determines whether storage can fulfil the query
type Storage func(query storage.Query, store storage.Storage) bool

// LocalOnly filters out all remote storages
func LocalOnly(_ storage.Query, store storage.Storage) bool {
	return store.Type() == storage.TypeLocalDC
}

// RemoteOnly filters out any non-remote storages
func RemoteOnly(_ storage.Query, store storage.Storage) bool {
	return store.Type() == storage.TypeRemoteDC
}

// AllowAll does not filter any storages
func AllowAll(_ storage.Query, _ storage.Storage) bool {
	return true
}

// AllowNone filters all storages
func AllowNone(_ storage.Query, _ storage.Storage) bool {
	return false
}

// Allow only storages which meet the relevant filters in the query.
func ReadOptimizedFilter(query storage.Query, store storage.Storage) bool {
	if store.Name() == localStorageName {
		return true
	}
	fetchQuery, ok := query.(*storage.FetchQuery)
	if !ok {
		// This filter only applies to fetch queries. The configration is wrong!
		return true
	}	
	for _, tagMatcher := range fetchQuery.TagMatchers {
		if string(tagMatcher.Name) == storageNameLabelKey {
			switch tagMatcher.Type {
			// NB: This is a bit hacky. The storage name is like "remote_store_prod-aws-nvirginia-prod", while the tag matcher is like "shardName=nvirginia-prod".
			case models.MatchEqual:
				if !strings.HasSuffix(store.Name(), string(tagMatcher.Value)) {
					return false
				}
			}
		}
	}
	return true
}

// StorageCompleteTags determines whether storage can fulfil the complete tag query
type StorageCompleteTags func(query storage.CompleteTagsQuery, store storage.Storage) bool

// CompleteTagsLocalOnly filters out all remote storages
func CompleteTagsLocalOnly(_ storage.CompleteTagsQuery, store storage.Storage) bool {
	return store.Type() == storage.TypeLocalDC
}

// CompleteTagsRemoteOnly filters out any non-remote storages
func CompleteTagsRemoteOnly(_ storage.CompleteTagsQuery, store storage.Storage) bool {
	return store.Type() == storage.TypeRemoteDC
}

// CompleteTagsAllowAll does not filter any storages
func CompleteTagsAllowAll(_ storage.CompleteTagsQuery, _ storage.Storage) bool {
	return true
}

// CompleteTagsAllowNone filters all storages
func CompleteTagsAllowNone(_ storage.CompleteTagsQuery, _ storage.Storage) bool {
	return false
}
