// Copyright (c) 2019 Uber Technologies, Inc.
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

package main

// This .go file is used to test the lockfile package (m3/src/x/lockfile)

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/m3db/m3/src/x/lockfile"
)

func exitWithUsage() {
	fmt.Printf(
		"Usage: %[1]s <LOCK_FILE_PATH> <NUM_SECONDS_TO_SLEEP> <SHOULD_CLEANUP_LOCK>\nExample: %[1]s /var/run/lockfile 1 1\n",
		path.Base(os.Args[0]))
	os.Exit(1)
}

func main() {
	if len(os.Args) != 4 {
		exitWithUsage()
	}

	path, sleepStr, rmLock := os.Args[1], os.Args[2], os.Args[3]
	sleep, err := strconv.Atoi(sleepStr)
	if err != nil {
		exitWithUsage()
	}

	lock, err := lockfile.Acquire(path)
	if err != nil {
		os.Exit(1)
	}

	if sleep > 0 {
		time.Sleep(time.Duration(sleep) * time.Second)
	}

	if rmLock != "0" {
		lock.Release()
	}
}
