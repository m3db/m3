// Copyright (c) 2020  Uber Technologies, Inc.
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

package namespace

import (
	"fmt"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
)

// StagingState is the state associated with a namespace's
// availability for reads and writes.
type StagingState struct {
	status StagingStatus
}

// Status returns the StagingStatus for a namespace.
func (s StagingState) Status() StagingStatus {
	return s.status
}

// Validate validates the StagingState object.
func (s StagingState) Validate() error {
	var validStatus bool
	for _, status := range validStagingStatuses {
		if status == s.Status() {
			validStatus = true
			break
		}
	}
	if !validStatus {
		return fmt.Errorf("staging state status %v is invalid", s.Status())
	}

	return nil
}

// NewStagingState creates a new StagingState.
func NewStagingState(status nsproto.StagingStatus) (StagingState, error) {
	switch status {
	case nsproto.StagingStatus_UNKNOWN:
		return StagingState{status: UnknownStagingStatus}, nil
	case nsproto.StagingStatus_INITIALIZING:
		return StagingState{status: InitializingStagingStatus}, nil
	case nsproto.StagingStatus_READY:
		return StagingState{status: ReadyStagingStatus}, nil
	}
	return StagingState{}, fmt.Errorf("invalid namespace status: %v", status)
}

func (s StagingStatus) String() string {
	switch s {
	case ReadyStagingStatus:
		return "ready"
	case InitializingStagingStatus:
		return "initializing"
	default:
		return "unknown"
	}
}
