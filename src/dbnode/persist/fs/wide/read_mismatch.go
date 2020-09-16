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
	"bytes"
	"fmt"
)

// Cleanup releases any resources held by this mismatch.
func (rm *ReadMismatch) Cleanup() {
	if rm.cleanup == nil {
		return
	}

	rm.cleanup()
}

// String pretty-prints a ReadMismatch.
func (rm ReadMismatch) String() string {
	if rm.ID == nil {
		return fmt.Sprintf("{Mismatched: %t, Checksum: %d}", rm.Mismatched, rm.Checksum)
	}

	if rm.Data != nil {
		return fmt.Sprintf(
			"{Mismatched: %t, Checksum: %d, ID: %s, EncodedTags: %s, Data: %s}",
			rm.Mismatched, rm.Checksum, rm.ID, rm.EncodedTags, rm.Data.Bytes())
	}

	return fmt.Sprintf(
		"{Mismatched: %t, Checksum: %d, ID: %s, EncodedTags: %s, Data: <nil>}",
		rm.Mismatched, rm.Checksum, rm.ID, rm.EncodedTags)
}

// Equal determines if two ReadMismatches are equal.
func (rm ReadMismatch) Equal(other ReadMismatch) bool {
	match := rm.Mismatched == other.Mismatched &&
		rm.Checksum == other.Checksum &&
		bytes.Equal(rm.ID, other.ID) &&
		bytes.Equal(rm.EncodedTags, other.EncodedTags)
	if !match {
		return false
	}

	if rm.Data == nil {
		if other.Data == nil {
			return true
		}
		return false
	}

	return other.Data != nil &&
		bytes.Equal(rm.Data.Bytes(), other.Data.Bytes())
}

// AddCleanupStep adds an additional cleanup step to this read mismatch.
func (rm *ReadMismatch) AddCleanupStep(fn Cleanup) {
	if fn == nil {
		return
	}

	previousCleanup := rm.cleanup
	if previousCleanup == nil {
		rm.cleanup = fn
		return
	}

	rm.cleanup = func() {
		previousCleanup()
		fn()
	}
}
