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
		return 0
	}
	return cpus[CPU()]
}
