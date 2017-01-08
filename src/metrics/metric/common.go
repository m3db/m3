// Copyright (c) 2016 Uber Technologies, Inc.
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

package metric

import "fmt"

// ID is the metric id
// TODO(xichen): make ID a union of numeric ID and bytes-backed IDs
// so we can compress IDs on a per-connection basis
type ID []byte

// String is the string representation of an id
func (id ID) String() string { return string(id) }

// ChunkedID is a three-part id
type ChunkedID struct {
	Prefix []byte
	Data   []byte
	Suffix []byte
}

// String is the string representation of the chunked id
func (cid ChunkedID) String() string {
	return fmt.Sprintf("%s%s%s", cid.Prefix, cid.Data, cid.Suffix)
}
