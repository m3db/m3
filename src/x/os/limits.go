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

// ProcessLimits captures the different process limits.
type ProcessLimits struct {
	NoFileCurr    uint64 // RLIMIT_NOFILE Current
	NoFileMax     uint64 // RLIMIT_NOFILE Max
	VMMaxMapCount int64  // corresponds to /proc/sys/vm/max_map_count
	VMSwappiness  int64  // corresponds to /proc/sys/vm/swappiness
}

// RaiseProcessNoFileToNROpenResult captures the result of trying to
// raise the process num files open limit to the nr_open system value.
type RaiseProcessNoFileToNROpenResult struct {
	RaisePerformed  bool
	NROpenValue     uint64
	NoFileMaxValue  uint64
	NoFileCurrValue uint64
}
