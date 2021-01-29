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
	"sync/atomic"
)

const (
	// ProfileFileExtension is the extension of profile files.
	ProfileFileExtension = ".pb.gz"
)

// FileProfiler is profiler which writes its profiles to given path directory.
type FileProfiler struct {
	path         string
	profileNames map[string]*profileName
}

// NewFileProfiler creates a new file provider.
func NewFileProfiler(path string) *FileProfiler {
	return &FileProfiler{
		path:         path,
		profileNames: make(map[string]*profileName),
	}
}

// StartCPUProfile starts named cpu profile.
func (f FileProfiler) StartCPUProfile(name string) error {
	profileName := f.profileName(name)
	return startCPUProfile(f.path, profileName)
}

// StopCPUProfile stops started cpu profile.
func (f FileProfiler) StopCPUProfile() error {
	stopCPUProfile()
	return nil
}

// WriteHeapProfile writes named heap profile.
func (f FileProfiler) WriteHeapProfile(name string) error {
	profileName := f.profileName(name)
	return writeHeapProfile(f.path, profileName)
}

func (f FileProfiler) profileName(name string) *profileName {
	profileName, ok := f.profileNames[name]
	if !ok {
		profileName = newProfileName(name)
		f.profileNames[name] = profileName
	}
	return profileName
}

type profileType int

// String returns string representation of profile type.
func (p profileType) String() string {
	switch p {
	case cpuProfile:
		return "cpu"
	case heapProfile:
		return "heap"
	default:
		return ""
	}
}

const (
	cpuProfile profileType = iota
	heapProfile
)

type profileName struct {
	name   string
	counts map[profileType]*int32
}

func newProfileName(name string) *profileName {
	return &profileName{
		name:   name,
		counts: make(map[profileType]*int32),
	}
}

func (p *profileName) inc(pType profileType) int32 {
	count, ok := p.counts[pType]
	if !ok {
		count = new(int32)
		p.counts[pType] = count
	}
	return atomic.AddInt32(count, 1)
}

func (p *profileName) withProfileType(pType profileType) string {
	return p.name + "." + pType.String()
}

func startCPUProfile(path string, profileName *profileName) error {
	file, err := newProfileFile(path, profileName, cpuProfile)
	if err != nil {
		return err
	}

	for {
		if err := pprof.StartCPUProfile(file); err != nil {
			// cpu profile is already started, so we stop it and start our own.
			pprof.StopCPUProfile()
			continue
		}
		return nil
	}
}

func stopCPUProfile() {
	pprof.StopCPUProfile()
}

func writeHeapProfile(path string, profileName *profileName) error {
	file, err := newProfileFile(path, profileName, heapProfile)
	if err != nil {
		return err
	}

	return pprof.WriteHeapProfile(file)
}

// newProfileFile creates new file for writing bootstrap profile.
// path is a directory where profile files will be put.
// If path is empty string, temp directory will be used instead.
func newProfileFile(path string, profileName *profileName, pType profileType) (*os.File, error) {
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

	for {
		filename := fmt.Sprintf("%s.%d%s",
			profileName.withProfileType(pType), profileName.inc(pType), ProfileFileExtension)
		// fails if a file with given name exists
		if file, err := os.OpenFile(filepath.Clean(filepath.Join(path, filename)), os.O_CREATE|os.O_EXCL, 0600); err == nil {
			return file, nil
		}
	}
}
