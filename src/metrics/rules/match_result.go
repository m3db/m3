// Copyright (c) 2020 Uber Technologies, Inc.
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

package rules

import (
	"bytes"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
)

// EmptyMatchResult is the result when no matches were found.
var EmptyMatchResult = NewMatchResult(
	kv.UninitializedVersion,
	timeNanosMax,
	metadata.DefaultStagedMetadatas,
	nil,
	false,
)

// IDWithMetadatas is a pair of metric ID and the associated staged metadatas.
type IDWithMetadatas struct {
	ID        []byte
	Metadatas metadata.StagedMetadatas
}

// Reset resets an IDWithMetadatas for reuse.
func (i *IDWithMetadatas) Reset() {
	i.ID = i.ID[:0]
	for idx := range i.Metadatas {
		i.Metadatas[idx].Reset()
	}
	i.Metadatas = i.Metadatas[:0]
}

// Clone creates a copy of IDWithMetadatas.
func (i IDWithMetadatas) Clone() IDWithMetadatas {
	clone := i
	clone.ID = make([]byte, 0, len(i.ID))
	clone.ID = append(clone.ID, i.ID...)
	clone.Metadatas = i.Metadatas.Clone()
	return clone
}

// IDWithMetadatasByIDAsc sorts a list of ID with metadatas by metric ID in ascending order.
type IDWithMetadatasByIDAsc []IDWithMetadatas

func (a IDWithMetadatasByIDAsc) Len() int           { return len(a) }
func (a IDWithMetadatasByIDAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a IDWithMetadatasByIDAsc) Less(i, j int) bool { return bytes.Compare(a[i].ID, a[j].ID) < 0 }

// MatchResult represents a match result.
type MatchResult struct {
	version       int
	expireAtNanos int64
	// This contains the matched staged metadatas where the metric ID
	// remains the same during the first step of the aggregation process,
	// which includes all mapping rule matches, as well as rollup rule
	// matches where the first pipeline operation is not a rollup operation.
	forExistingID metadata.StagedMetadatas
	// This contains the match result where a new metric ID is used
	// during the first step of the aggregation process, which is usually
	// produced by a rollup rule whose rollup pipeline contains a rollup operation
	// as its first step.
	forNewRollupIDs []IDWithMetadatas
	keepOriginal    bool
}

// MatchOptions are request level options for each Match.
type MatchOptions struct {
	NameAndTagsFn       id.NameAndTagsFn
	SortedTagIteratorFn id.SortedTagIteratorFn

	// MatchResult is the final MatchResult that will
	// be updated by the ForwardMatch logic.
	MatchResult *MatchResult
}

// NewMatchResult creates a new match result.
func NewMatchResult(
	version int,
	expireAtNanos int64,
	forExistingID metadata.StagedMetadatas,
	forNewRollupIDs []IDWithMetadatas,
	keepOriginal bool,
) MatchResult {
	return MatchResult{
		version:         version,
		expireAtNanos:   expireAtNanos,
		forExistingID:   forExistingID,
		forNewRollupIDs: forNewRollupIDs,
		keepOriginal:    keepOriginal,
	}
}

// Version returns the version of the match result.
func (r *MatchResult) Version() int { return r.version }

// ExpireAtNanos returns the expiration time of the match result in nanoseconds.
func (r *MatchResult) ExpireAtNanos() int64 { return r.expireAtNanos }

// HasExpired returns whether the match result has expired for a given time.
func (r *MatchResult) HasExpired(timeNanos int64) bool { return r.expireAtNanos <= timeNanos }

// NumNewRollupIDs returns the number of new rollup metric IDs generated as a
// result of rule matching.
func (r *MatchResult) NumNewRollupIDs() int { return len(r.forNewRollupIDs) }

// ForExistingIDAt returns the staged metadatas for existing ID at a given time.
func (r *MatchResult) ForExistingIDAt(timeNanos int64) metadata.StagedMetadatas {
	return activeStagedMetadatasAt(r.forExistingID, timeNanos)
}

// ForNewRollupIDsAt returns the the new rollup ID alongside its staged metadatas
// for a given index at a given time.
func (r *MatchResult) ForNewRollupIDsAt(idx int, timeNanos int64) IDWithMetadatas {
	forNewRollupID := r.forNewRollupIDs[idx]
	metadatas := activeStagedMetadatasAt(forNewRollupID.Metadatas, timeNanos)
	return IDWithMetadatas{ID: forNewRollupID.ID, Metadatas: metadatas}
}

// KeepOriginal returns true if the original source metric for a rollup rule
// should be kept, and false if it should be dropped.
func (r *MatchResult) KeepOriginal() bool {
	return r.keepOriginal
}

// Clone creates a copy of the MatchResult.
func (r MatchResult) Clone() MatchResult {
	clone := r
	clone.forNewRollupIDs = make([]IDWithMetadatas, 0, len(r.forNewRollupIDs))
	for _, idWithMetadata := range r.forNewRollupIDs {
		clone.forNewRollupIDs = append(clone.forNewRollupIDs, idWithMetadata.Clone())
	}
	clone.forExistingID = r.forExistingID.Clone()
	return clone
}

// Reset resets a MatchResult so that it can be reused.
func (r *MatchResult) Reset() {
	r.version = 0
	r.expireAtNanos = 0
	r.keepOriginal = false

	for i := range r.forExistingID {
		r.forExistingID[i].Reset()
	}
	r.forExistingID = r.forExistingID[:0]

	for i := range r.forNewRollupIDs {
		r.forNewRollupIDs[i].Reset()
	}
	r.forNewRollupIDs = r.forNewRollupIDs[:0]
}

// activeStagedMetadatasAt returns the active staged metadatas at a given time, assuming
// the input list of staged metadatas are sorted by cutover time in ascending order.
func activeStagedMetadatasAt(
	metadatas metadata.StagedMetadatas,
	timeNanos int64,
) metadata.StagedMetadatas {
	for idx := len(metadatas) - 1; idx >= 0; idx-- {
		if metadatas[idx].CutoverNanos <= timeNanos {
			return metadatas[idx:]
		}
	}
	return metadatas
}
