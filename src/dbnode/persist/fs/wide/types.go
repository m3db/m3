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
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package wide

import (
	"github.com/m3db/m3/src/x/checked"
)

// ReadMismatch describes a metric that does not match a given summary,
// with descriptor of the mismatch.
type ReadMismatch struct {
	// Checksum is the checksum for the mismatched series.
	Checksum int64
	// Data is the data for this query.
	// NB: only present for MismatchMissingInSummary and MismatchChecksumMismatch.
	Data []checked.Bytes
	// EncodedTags are the tags for this query.
	// NB: only present for MismatchMissingInSummary and MismatchChecksumMismatch.
	EncodedTags []byte
	// ID is the ID for this query.
	// NB: only present for MismatchMissingInSummary.
	ID []byte

	cleanup cleanup
}

type cleanup func()

// Cleanup releases any resources held by this mismatch
func (rm *ReadMismatch) Cleanup() {
	if rm.cleanup == nil {
		return
	}

	rm.cleanup()
}
