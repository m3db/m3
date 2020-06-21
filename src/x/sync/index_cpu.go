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

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

var (
	cpus     []int
	cpusRead bool
)

func init() {
	cpuinfo, err := ioutil.ReadFile("/proc/cpuinfo")
	if err != nil {
		return
	}

	var pnum int
	var apic uint64
	lines := strings.Split(string(cpuinfo), "\n")
	for i, line := range lines {
		if len(line) == 0 && i != 0 {
			if int(apic) >= len(cpus) {
				realloc := make([]int, 2*apic)
				copy(realloc, cpus)
				cpus = realloc
			}
			cpus[apic] = pnum
			pnum = 0
			apic = 0
			continue
		}

		fields := strings.Fields(line)

		switch fields[0] {
		case "processor":
			pnum, err = strconv.Atoi(fields[2])
		case "apicid":
			apic, err = strconv.ParseUint(fields[2], 10, 64)
		}

		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			return
		}
	}

	cpusRead = true
}

// IndexCPU returns the current CPU index.
func IndexCPU() int {
	if !cpusRead {
		return 0 // Likely not linux and nothing available in procinfo.
	}
	return cpus[CPU()]
}
