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

package postings

// Equal compares two postings lists for equality.
func Equal(a, b List) bool {
	countA, okA := a.CountFast()
	countB, okB := b.CountFast()
	if okA && okB && countA != countB {
		return false
	}

	iter := a.Iterator()
	otherIter := b.Iterator()

	closed := false
	defer func() {
		if !closed {
			_ = iter.Err()
			_ = iter.Close()
			_ = otherIter.Err()
			_ = otherIter.Close()
		}
	}()

	for iter.Next() {
		if !otherIter.Next() {
			return false
		}
		if iter.Current() != otherIter.Current() {
			return false
		}
	}

	if otherIter.Next() {
		// Other iterator still had values.
		return false
	}

	closed = true
	iterErr := iter.Err()
	iterClose := iter.Close()
	otherIterErr := otherIter.Err()
	otherIterClose := otherIter.Close()
	return iterErr == nil &&
		iterClose == nil &&
		otherIterErr == nil &&
		otherIterClose == nil
}

// CountSlow returns the count of postings list values by iterating.
func CountSlow(a List) int {
	count := 0
	iter := a.Iterator()
	for iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		return 0
	}
	if err := iter.Close(); err != nil {
		return 0
	}
	return count
}
