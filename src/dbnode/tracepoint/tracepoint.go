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

package tracepoint

// The tracepoint package is used to store operation names for tracing throughout dbnode.
// The naming convention is as follows:

// `packageName.objectName.method`

// If there isn't an object, use `packageName.method`.

const (
	// FetchTagged is the operation name for the tchannelthrift FetchTagged path.
	FetchTagged = "tchannelthrift/node.service.FetchTagged"

	// Query is the operation name for the tchannelthrift Query path.
	Query = "tchannelthrift/node.service.Query"

	// DBQueryIDs is the operation name for the db QueryIDs path.
	DBQueryIDs = "storage.db.QueryIDs"

	// NSQueryIDs is the operation name for the dbNamespace QueryIDs path.
	NSQueryIDs = "storage.dbNamespace.QueryIDs"
)
