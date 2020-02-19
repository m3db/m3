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

package tracepoint

// The tracepoint package is used to store operation names for tracing throughout query.
// The naming convention is as follows:

// `packageName.objectName.method`

// If there isn't an object, use `packageName.method`.

const (
	// FetchCompressedFetchTagged is for the call to FetchTagged in fetchCompressed.
	FetchCompressedFetchTagged = "m3.m3storage.fetchCompressed.FetchTagged"

	// SearchCompressedFetchTaggedIDs is for the call to FetchTaggedIDs in SearchCompressed.
	SearchCompressedFetchTaggedIDs = "m3.m3storage.SearchCompressed.FetchTaggedIDs"

	// CompleteTagsAggregate is for the call to Aggregate in CompleteTags.
	CompleteTagsAggregate = "m3.m3storage.CompleteTags.Aggregate"

	// TemporalDecodeSingle is time taken for a single pass decode time.
	TemporalDecodeSingle = "temporal.singleProcess.decode"

	// TemporalDecodeParallel is time taken for a parallel pass decode time.
	TemporalDecodeParallel = "temporal.parallelProcess.decode"
)
