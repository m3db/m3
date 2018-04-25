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

package client

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/topology"
	xerrors "github.com/m3db/m3x/errors"
)

type fetchResultsAccumulator interface {
	// Reset resets the accumulator for use.
	Reset(
		startTime time.Time,
		endTime time.Time,
		topoMap topology.Map,
		majority int,
		consistencyLevel topology.ReadConsistencyLevel)

	// Add adds the provided result to the accumulator.
	Add(opts fetchTaggedResultAccumulatorOpts, err error) (done bool, addErr error)

	// AsEncodingSeriesIterators converts the underlying response into an encoding.SeriesIterators
	AsEncodingSeriesIterators(limit int, pools fetchTaggedPools) (
		iters encoding.SeriesIterators, exhaustive bool, err error)

	// AsIndexQueryResults converts the underlying response into an index.QueryResults
	AsIndexQueryResults(limit int, pools fetchTaggedPools) (index.QueryResults, error)

	// Clear clears any internal state, and releases references the object
	// has to external entities. It does not release the object itself.
	Clear()
}

type fetchTaggedResultAccumulatorOpts struct {
	host     topology.Host
	response *rpc.FetchTaggedResult_
}

// make the compiler ensure the concrete type `*fetchTaggedResultAccumulator` implements
// the `fetchResultsAccumulator` interface.
var _ fetchResultsAccumulator = &fetchTaggedResultAccumulator{}

type fetchTaggedResultAccumulator struct {
	// NB(prateek): a fetchTagged request requires we fan out to each shard in the
	// topology. As a result, we track the response consistency per shard.
	// Length of this slice == 1 + max shard id in topology
	shardConsistencyResults []fetchTaggedShardConsistencyResult
	numHostsPending         int32
	numShardsPending        int32

	errors     xerrors.Errors
	responses  fetchTaggedIDResults
	exhaustive bool

	startTime        time.Time
	endTime          time.Time
	majority         int
	consistencyLevel topology.ReadConsistencyLevel
	topoMap          topology.Map
}

type fetchTaggedShardConsistencyResult struct {
	enqueued int8
	success  int8
	errors   int8
	done     bool
}

func (accum *fetchTaggedResultAccumulator) Add(
	opts fetchTaggedResultAccumulatorOpts,
	resultErr error,
) (bool, error) {
	host := opts.host
	response := opts.response

	accum.numHostsPending--
	if resultErr != nil {
		accum.errors = append(accum.errors, xerrors.NewRenamedError(resultErr,
			fmt.Errorf("error fetching tagged from host %s", host.ID())))
	} else {
		accum.exhaustive = accum.exhaustive && response.Exhaustive
		for _, elem := range response.Elements {
			accum.responses = append(accum.responses, elem)
		}
	}

	hostShardSet, ok := accum.topoMap.LookupHostShardSet(host.ID())
	if !ok {
		// should never happen, as we've taken a reference to the
		// topology when beginning the request, and the var is immutable.
		doneAccumulating := true
		err := fmt.Errorf(
			"[invariant violated] missing host shard in fetchState completionFn: %s", host.ID())
		return doneAccumulating, xerrors.NewNonRetryableError(err)
	}

	for _, hs := range hostShardSet.ShardSet().All() {
		if hs.State() != shard.Available {
			// Currently, we only accept responses from shard's which are available
			// NB: as a possible enhancement, we could accept a response from
			// a shard that's not available if we tracked response pairs from
			// a LEAVING+INITIALIZING shard; this would help during node replaces.
			continue
		}

		shardID := int(hs.ID())
		shardResult := accum.shardConsistencyResults[shardID]
		if shardResult.done {
			continue // already been marked done, don't need to do anything for this shard
		}

		if resultErr == nil {
			shardResult.success++
		} else {
			shardResult.errors++
		}

		shardResult.done = readConsistencyAchieved(accum.consistencyLevel, accum.majority,
			int(shardResult.enqueued), int(shardResult.success))

		if shardResult.done {
			accum.numShardsPending--
		}

		// update value in slice
		accum.shardConsistencyResults[shardID] = shardResult
	}

	// success case, sufficient responses for each shard
	if accum.numShardsPending == 0 {
		doneAccumulating := true
		return doneAccumulating, nil
	}

	// failure case - we've received all responses but still weren't able to satisfy
	// all shards, so we need to fail
	if accum.numHostsPending == 0 && accum.numShardsPending != 0 {
		doneAccumulating := true
		return doneAccumulating, fmt.Errorf(
			"unable to satisfy consistency requirements for %d shards [ err = %s ]",
			accum.numShardsPending, accum.errors.Error())
	}

	doneAccumulating := false
	return doneAccumulating, nil
}

func (accum *fetchTaggedResultAccumulator) Clear() {
	for i := range accum.responses {
		accum.responses[i] = nil
	}
	accum.responses = accum.responses[:0]
	for i := range accum.errors {
		accum.errors[i] = nil
	}
	accum.errors = accum.errors[:0]
	accum.shardConsistencyResults = accum.shardConsistencyResults[:0]
	accum.consistencyLevel = topology.ReadConsistencyLevelNone
	accum.majority, accum.numHostsPending, accum.numShardsPending = 0, 0, 0
	accum.startTime, accum.endTime = time.Time{}, time.Time{}
	accum.topoMap = nil
}

func (accum *fetchTaggedResultAccumulator) Reset(
	startTime time.Time,
	endTime time.Time,
	topoMap topology.Map,
	majority int,
	consistencyLevel topology.ReadConsistencyLevel,
) {
	accum.exhaustive = true
	accum.startTime = startTime
	accum.endTime = endTime
	accum.topoMap = topoMap
	accum.majority = majority
	accum.consistencyLevel = consistencyLevel
	accum.numHostsPending = int32(topoMap.HostsLen())
	accum.numShardsPending = int32(len(topoMap.ShardSet().All()))

	// expand shardResults as much as necessary
	targetLen := 1 + int(topoMap.ShardSet().Max())
	accum.shardConsistencyResults = fetchTaggedShardConsistencyResults(accum.shardConsistencyResults).initialize(targetLen)
	// initialize shardResults based on current topology
	for _, hss := range topoMap.HostShardSets() {
		for _, hShard := range hss.ShardSet().All() {
			id := int(hShard.ID())
			accum.shardConsistencyResults[id].enqueued++
		}
	}
}

func (accum *fetchTaggedResultAccumulator) sliceResponsesAsSeriesIter(
	pools fetchTaggedPools,
	elems fetchTaggedIDResults,
) encoding.SeriesIterator {
	numElems := len(elems)
	iters := pools.IteratorArray().Get(numElems)[:numElems]
	for idx, elem := range elems {
		slicesIter := pools.ReaderSliceOfSlicesIterator().Get()
		slicesIter.Reset(elem.Segments)
		multiIter := pools.MultiReaderIterator().Get()
		multiIter.ResetSliceOfSlices(slicesIter)
		iters[idx] = multiIter
	}

	// pick the first element as they all have identical ids/tags
	// NB: safe to assume this element exists as it's only called within
	// a forEachID lambda, which provides the guarantee that len(elems) != 0
	elem := elems[0]

	encodedTags := pools.CheckedBytesWrapper().Get(elem.EncodedTags)
	decoder := pools.TagDecoder().Get()
	decoder.Reset(encodedTags)

	tsID := pools.CheckedBytesWrapper().Get(elem.ID)
	nsID := pools.CheckedBytesWrapper().Get(elem.NameSpace)
	seriesIter := pools.SeriesIterator().Get()
	seriesIter.Reset(pools.ID().BinaryID(tsID), pools.ID().BinaryID(nsID),
		decoder, accum.startTime, accum.endTime, iters)

	return seriesIter
}

func (accum *fetchTaggedResultAccumulator) AsEncodingSeriesIterators(
	limit int, pools fetchTaggedPools,
) (encoding.SeriesIterators, bool, error) {
	results := fetchTaggedIDResultsSortedByID(accum.responses)
	sort.Sort(results)
	accum.responses = fetchTaggedIDResults(results)

	numElements := 0
	accum.responses.forEachID(func(_ fetchTaggedIDResults) bool {
		numElements++
		return numElements < limit
	})

	result := pools.MutableSeriesIterators().Get(numElements)
	result.Reset(numElements)
	count := 0
	accum.responses.forEachID(func(elems fetchTaggedIDResults) bool {
		seriesIter := accum.sliceResponsesAsSeriesIter(pools, elems)
		result.SetAt(count, seriesIter)
		count++
		return count < limit
	})

	exhaustive := accum.exhaustive && count <= limit
	return result, exhaustive, nil
}

func (accum *fetchTaggedResultAccumulator) AsIndexQueryResults(
	limit int,
	pools fetchTaggedPools,
) (index.QueryResults, error) {
	var (
		iter  = newFetchTaggedResultsIndexIterator(pools)
		count = 0
	)
	results := fetchTaggedIDResultsSortedByID(accum.responses)
	sort.Sort(results)
	accum.responses = fetchTaggedIDResults(results)
	accum.responses.forEachID(func(elems fetchTaggedIDResults) bool {
		iter.backing.ids = append(iter.backing.ids, elems[0].ID)
		iter.backing.nses = append(iter.backing.nses, elems[0].NameSpace)
		iter.backing.tags = append(iter.backing.tags, elems[0].EncodedTags)
		count++
		return count < limit
	})

	return index.QueryResults{
		Exhaustive: accum.exhaustive && count <= limit,
		Iterator:   iter,
	}, nil
}

type fetchTaggedShardConsistencyResults []fetchTaggedShardConsistencyResult

func (res fetchTaggedShardConsistencyResults) initialize(length int) fetchTaggedShardConsistencyResults {
	if cap(res) < length {
		res = append(res, make(fetchTaggedShardConsistencyResults, length-cap(res))...)
	}
	res = res[:length]
	// following compiler optimized memcpy impl:
	// https://github.com/golang/go/wiki/CompilerOptimizations#optimized-memclr
	for i := range res {
		res[i] = fetchTaggedShardConsistencyResult{}
	}
	return res
}

type fetchTaggedIDResults []*rpc.FetchTaggedIDResult_

// lambda to iterate over fetchTagged responses a single id at a time,
// the returned bool indicates if the iteration should be continued past the
// curent id.
type forEachFetchTaggedIDFn func(responsesForSingleID fetchTaggedIDResults) bool

// forEachID iterates over the provide results, and calls `fn` on each
// group of responses with the same ID.
// NB: assumes the results array being operated upon has been sorted
func (results fetchTaggedIDResults) forEachID(fn forEachFetchTaggedIDFn) {
	var (
		startIdx = 0
		lastID   []byte
	)
	for i := 0; i < len(results); i++ {
		elem := results[i]
		if !bytes.Equal(elem.ID, lastID) {
			// We only want to call the the forEachID fn once we have calculated the entire group,
			// i.e. once we have gone past the last element for a given ID, but the first element
			// in the results slice is a special case because we are always starting a new group
			// at that point.
			if i == 0 {
				lastID = elem.ID
				continue
			}
			continueIterating := fn(results[startIdx:i])
			if !continueIterating {
				return
			}
			lastID = elem.ID
			startIdx = i
		}
	}
	// spill over
	if startIdx < len(results) {
		fn(results[startIdx:])
	}
}

// fetchTaggedIDResultsSortedByID implements sort.Interface for fetchTaggedIDResults
// based on the ID field.
type fetchTaggedIDResultsSortedByID fetchTaggedIDResults

func (a fetchTaggedIDResultsSortedByID) Len() int      { return len(a) }
func (a fetchTaggedIDResultsSortedByID) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a fetchTaggedIDResultsSortedByID) Less(i, j int) bool {
	return bytes.Compare(a[i].ID, a[j].ID) < 0
}
