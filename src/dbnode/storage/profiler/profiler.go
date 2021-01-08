// Copyright (c) 2021 Uber Technologies, Inc.
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

// Package profiler contains the code used for profiling.
package profiler

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/pprof"
)

// StartCPUProfile starts cpu profile.
func StartCPUProfile(path string, profileNamePrefix ProfileNamePrefix) error {
	file, err := newProfileFile(path, profileNamePrefix)
	if err != nil {
		return err
	}

	return pprof.StartCPUProfile(file)
}

// StopCPUProfile stops cpu profile.
func StopCPUProfile() {
	pprof.StopCPUProfile()
}

// WriteHeapProfile writes heap profile to file.
func WriteHeapProfile(path string, profileNamePrefix ProfileNamePrefix) error {
	file, err := newProfileFile(path, profileNamePrefix)
	if err != nil {
		return err
	}

	return pprof.WriteHeapProfile(file)
}

// newProfileFile creates new file for writing bootstrap profile.
// path is a directory where profile files will be put.
// If path is empty string, temp directory will be used instead.
func newProfileFile(path string, profileNamePrefix ProfileNamePrefix) (*os.File, error) {
	if path == "" {
		tmpDir, err := ioutil.TempDir("", "profile-")
		if err != nil {
			return nil, err
		}
		path = tmpDir
	}

	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}

	matches, err := filepath.Glob(filepath.Join(path,
		fmt.Sprintf("%s*%s", profileNamePrefix, ProfileFileExtension)))
	if err != nil {
		return nil, err
	}

	return os.Create(filepath.Join(path, fmt.Sprintf("%s%d%s",
		profileNamePrefix, len(matches), ProfileFileExtension)))
}
