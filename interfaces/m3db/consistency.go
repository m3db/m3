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

package m3db

// ConsistencyLevel is the consistency level for cluster operations
type ConsistencyLevel int

const (
	consistencyLevelNone ConsistencyLevel = iota

	// ConsistencyLevelOne corresponds to a single node participating
	// for an operation to succeed
	ConsistencyLevelOne

	// ConsistencyLevelMajority corresponds to the majority of nodes participating
	// for an operation to succeed
	ConsistencyLevelMajority

	// ConsistencyLevelAll corresponds to all nodes participating
	// for an operation to succeed
	ConsistencyLevelAll
)

// String returns the consistency level as a string
func (l ConsistencyLevel) String() string {
	switch l {
	case ConsistencyLevelOne:
		return "ConsistencyLevelOne"
	case ConsistencyLevelMajority:
		return "ConsistencyLevelMajority"
	case ConsistencyLevelAll:
		return "ConsistencyLevelAll"
	}
	return "ConsistencyLevelNone"
}
