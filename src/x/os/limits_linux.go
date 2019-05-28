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

package xos

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"syscall"
)

const (
	sysctlDir        = "/proc/sys/"
	vmMaxMapCountKey = "vm.max_map_count"
	vmSwappinessKey  = "vm.swappiness"
	fsNROpenKey      = "fs.nr_open"
)

// CanGetProcessLimits returns a boolean to signify if it can return limits,
// and a warning message if it cannot.
func CanGetProcessLimits() (bool, string) {
	return true, ""
}

// GetProcessLimits returns the known process limits.
func GetProcessLimits() (ProcessLimits, error) {
	var noFile syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &noFile)
	if err != nil {
		return ProcessLimits{}, err
	}

	maxMap, err := sysctlInt64(vmMaxMapCountKey)
	if err != nil {
		return ProcessLimits{}, err
	}

	swap, err := sysctlInt64(vmSwappinessKey)
	if err != nil {
		return ProcessLimits{}, err
	}

	return ProcessLimits{
		NoFileCurr:    noFile.Cur,
		NoFileMax:     noFile.Max,
		VMMaxMapCount: maxMap,
		VMSwappiness:  swap,
	}, nil
}

// RaiseProcessNoFileToNROpen first determines the NROpen limit by reading
// the corresponding proc sys file and then if the hard or soft limits
// are below this number, the limits are raised using a call to setrlimit.
func RaiseProcessNoFileToNROpen() (RaiseProcessNoFileToNROpenResult, error) {
	value, err := sysctlInt64(fsNROpenKey)
	if err != nil {
		return RaiseProcessNoFileToNROpenResult{}, fmt.Errorf(
			"unable to raise nofile limits: nr_open_parse_err=%v", err)
	}

	limit := uint64(value)

	var limits syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limits); err != nil {
		return RaiseProcessNoFileToNROpenResult{}, fmt.Errorf(
			"unable to raise nofile limits: rlimit_get_err=%v", err)
	}

	if limits.Max >= limit && limits.Cur >= limit {
		// Limit already set correctly
		return RaiseProcessNoFileToNROpenResult{
			RaisePerformed:  false,
			NROpenValue:     limit,
			NoFileMaxValue:  limits.Max,
			NoFileCurrValue: limits.Cur,
		}, nil
	}

	limits.Max = limit
	limits.Cur = limit

	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limits); err != nil {
		return RaiseProcessNoFileToNROpenResult{}, fmt.Errorf(
			"unable to raise nofile limits: rlimit_set_err=%v", err)
	}

	return RaiseProcessNoFileToNROpenResult{
		RaisePerformed:  true,
		NROpenValue:     limit,
		NoFileMaxValue:  limits.Max,
		NoFileCurrValue: limits.Cur,
	}, nil
}

func sysctlInt64(key string) (int64, error) {
	str, err := sysctl(key)
	if err != nil {
		return 0, err
	}

	num, err := strconv.Atoi(str)
	if err != nil {
		return 0, err
	}

	return int64(num), nil
}

func sysctl(key string) (string, error) {
	path := sysctlDir + strings.Replace(key, ".", "/", -1)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("could not find the given sysctl file: %v, err: %v", path, err)
	}
	return strings.TrimSpace(string(data)), nil
}
