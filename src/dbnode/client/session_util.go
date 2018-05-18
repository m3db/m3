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

	"github.com/m3db/m3db/src/dbnode/topology"
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

// readConsistencyTermination returns a bool to indicate whether sufficient
// responses (error/success) have been received, so that we're able to decide
// whether we will be able to satisfy the reuquest or not.
// NB: it is not the same as `readConsistencyAchieved`.
func readConsistencyTermination(
	level topology.ReadConsistencyLevel,
	majority, remaining, success int32,
) bool {
	doneAll := remaining == 0
	switch level {
	case topology.ReadConsistencyLevelOne, topology.ReadConsistencyLevelNone:
		return success > 0 || doneAll
	case topology.ReadConsistencyLevelMajority, topology.ReadConsistencyLevelUnstrictMajority:
		return success >= majority || doneAll
	case topology.ReadConsistencyLevelAll:
		return doneAll
	}
	panic(fmt.Errorf("unrecognized consistency level: %s", level.String()))
}

// `readConsistencyAchieved` returns whether sufficient responses have been received
// to reach the desired consistency.
// NB: it is not the same as `readConsistencyTermination`.
func readConsistencyAchieved(
	level topology.ReadConsistencyLevel,
	majority, enqueued, success int,
) bool {
	switch level {
	case topology.ReadConsistencyLevelAll:
		return success == enqueued // Meets all
	case topology.ReadConsistencyLevelMajority:
		return success >= majority // Meets majority
	case topology.ReadConsistencyLevelOne, topology.ReadConsistencyLevelUnstrictMajority:
		return success > 0 // Meets one
	case topology.ReadConsistencyLevelNone:
		return true // Always meets none
	}
	panic(fmt.Errorf("unrecognized consistency level: %s", level.String()))
}
