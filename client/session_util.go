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
	"fmt"

	"github.com/m3db/m3db/topology"
)

func writeConsistencyAchieved(
	level topology.ConsistencyLevel,
	majority, enqueued, success int,
) bool {
	switch level {
	case topology.ConsistencyLevelAll:
		if success == enqueued { // Meets all
			return true
		}
	case topology.ConsistencyLevelMajority:
		if success >= majority { // Meets majority
			return true
		}
	case topology.ConsistencyLevelOne:
		if success > 0 { // Meets one
			return true
		}
	default:
		panic(fmt.Errorf("unrecognized consistency level: %s", level.String()))
	}
	return false
}

func readConsistencyAchieved(
	level topology.ReadConsistencyLevel,
	majority, enqueued, success int,
) bool {
	switch level {
	case topology.ReadConsistencyLevelAll:
		if success == enqueued { // Meets all
			return true
		}
	case topology.ReadConsistencyLevelMajority:
		if success >= majority { // Meets majority
			return true
		}
	case topology.ReadConsistencyLevelOne, topology.ReadConsistencyLevelUnstrictMajority:
		if success > 0 { // Meets one
			return true
		}
	case topology.ReadConsistencyLevelNone:
		return true // Always meets none
	default:
		panic(fmt.Errorf("unrecognized consistency level: %s", level.String()))
	}
	return false
}
