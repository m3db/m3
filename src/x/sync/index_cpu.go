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

package sync

// CoreFn is a function that returns the ID of the
// CPU currently running this goroutine.
type CoreFn func() int

var numCores = 1

func init() {
	numCores = checkNumCores()
}

// NumCores returns the number of cores returned from
// OS-dependent checkNumCores(), if not available only returns 1
func NumCores() int {
	return numCores
}

// CPUCore returns the current CPU core.
func CPUCore() int {
	if numCores == 1 {
		// Likely not linux and nothing available in procinfo meaning that
		// even if RDTSCP is available we won't have setup correct number
		// of cores, etc for our queues since we probed using NumCores
		// and got 1 back.
		return 0
	}
	// We know the number of cores, try to call RDTSCP to get the core.
	return getCore()
}
