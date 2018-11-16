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

package server

import (
	"fmt"

	xos "github.com/m3db/m3/src/x/os"
	xerror "github.com/m3db/m3x/errors"
)

const (
	// TODO: determine these values based on topology/namespace configuration.
	minNoFile     = 500000
	minVMMapCount = 262144
	maxSwappiness = 1
)

func validateProcessLimits() error {
	limits, err := xos.GetProcessLimits()
	if err != nil {
		return fmt.Errorf("unable to determine process limits: %v", err)
	}

	var multiErr xerror.MultiError
	if limits.NoFileCurr < minNoFile {
		multiErr = multiErr.Add(fmt.Errorf(
			"current value for RLIMIT_NOFILE(%d) is below recommended threshold(%d)",
			limits.NoFileCurr, minNoFile,
		))
	}

	if limits.NoFileMax < minNoFile {
		multiErr = multiErr.Add(fmt.Errorf(
			"max value for RLIMIT_NOFILE(%d) is below recommended threshold(%d)",
			limits.NoFileMax, minNoFile,
		))
	}

	if limits.VmMaxMapCount < minVMMapCount {
		multiErr = multiErr.Add(fmt.Errorf(
			"current value for vm.max_map_count(%d) is below recommended threshold(%d)",
			limits.VmMaxMapCount, minVMMapCount,
		))
	}

	if limits.VmSwappiness > maxSwappiness {
		multiErr = multiErr.Add(fmt.Errorf(
			"current value for vm.swappiness(%d) is above recommended threshold(%d)",
			limits.VmSwappiness, maxSwappiness,
		))
	}

	return multiErr.FinalError()
}
