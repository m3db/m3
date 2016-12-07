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

package client

import "github.com/m3db/m3db/topology"

func writeConsistencyResult(
	writeLevel topology.ConsistencyLevel, majority, enqueued, pending int32,
	errs []error,
) error {
	numErrs := int32(len(errs))

	if numErrs == 0 {
		return nil
	}

	// Check consistency level satisfied
	responded := enqueued - pending
	success := enqueued - numErrs
	switch writeLevel {
	case topology.ConsistencyLevelAll:
		return newConsistencyResultError(writeLevel, int(enqueued), int(responded), errs)
	case topology.ConsistencyLevelMajority:
		if success >= majority {
			// Meets majority
			break
		}
		return newConsistencyResultError(writeLevel, int(enqueued), int(responded), errs)
	case topology.ConsistencyLevelOne:
		if success > 0 {
			// Meets one
			break
		}
		return newConsistencyResultError(writeLevel, int(enqueued), int(responded), errs)
	}

	return nil
}

func readConsistencyResult(
	readLevel ReadConsistencyLevel,
	majority, enqueued, responded, resultErrs int32,
	errs []error,
) error {
	if resultErrs == 0 {
		return nil
	}

	// Check consistency level satisfied
	success := enqueued - resultErrs
	switch readLevel {
	case ReadConsistencyLevelAll:
		return newConsistencyResultError(readLevel, int(enqueued), int(responded), errs)
	case ReadConsistencyLevelMajority:
		if success >= majority {
			// Meets majority
			break
		}
		return newConsistencyResultError(readLevel, int(enqueued), int(responded), errs)
	case ReadConsistencyLevelOne, ReadConsistencyLevelUnstrictMajority:
		if success > 0 {
			// Meets one
			break
		}
		return newConsistencyResultError(readLevel, int(enqueued), int(responded), errs)
	}

	return nil
}
