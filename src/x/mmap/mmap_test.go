// Copyright (c) 2017 Uber Technologies, Inc.
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

package mmap

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mmapFdFuncType func(fd, offset, length int64, opts Options) (Descriptor, error)

func TestMmapFile(t *testing.T) {
	fd, err := ioutil.TempFile("", "testfile")
	assert.NoError(t, err)

	desc, err := File(fd, Options{})
	assert.NoError(t, err)
	assert.NoError(t, desc.Warning)
	assert.Equal(t, []byte{}, desc.Bytes)

	Munmap(desc)
}

func TestMmapFiles(t *testing.T) {
	fd1, err := ioutil.TempFile("", "1")
	assert.NoError(t, err)
	fd1Path := fd1.Name()
	fd2, err := ioutil.TempFile("", "2")
	assert.NoError(t, err)
	fd2Path := fd2.Name()

	var (
		desc1 = Descriptor{}
		desc2 = Descriptor{}
	)
	result, err := Files(os.Open, map[string]FileDesc{
		fd1Path: FileDesc{
			File:       &fd1,
			Descriptor: &desc1,
			Options:    Options{},
		},
		fd2Path: FileDesc{
			File:       &fd2,
			Descriptor: &desc2,
			Options:    Options{},
		},
	})

	assert.NoError(t, err)
	assert.NoError(t, result.Warning)
}

func TestMmapFilesHandlesError(t *testing.T) {
	fd1, err := ioutil.TempFile("", "1")
	assert.NoError(t, err)
	fd1Path := fd1.Name()

	fd2, err := ioutil.TempFile("", "doesnt-matter")
	assert.NoError(t, err)
	var (
		desc1 = Descriptor{}
		desc2 = Descriptor{}
	)
	_, err = Files(os.Open, map[string]FileDesc{
		fd1Path: FileDesc{
			File:       &fd1,
			Descriptor: &desc1,
			Options:    Options{},
		},
		"does_not_exist": FileDesc{
			File:       &fd2,
			Descriptor: &desc2,
			Options:    Options{},
		},
	})

	assert.Error(t, err)
}

func TestMmapFilesHandlesWarnings(t *testing.T) {
	mmapFdReturnWarn := func(fd, offset, length int64, opts Options) (Descriptor, error) {
		return Descriptor{Warning: errors.New("some-error"), Bytes: []byte("a")}, nil
	}
	defer mockMmapFdFunc(mmapFdReturnWarn)()

	fd1, err := ioutil.TempFile("", "1")
	assert.NoError(t, err)
	fd1Path := fd1.Name()

	desc1 := Descriptor{}

	result, err := Files(os.Open, map[string]FileDesc{
		fd1Path: FileDesc{
			File:       &fd1,
			Descriptor: &desc1,
			Options:    Options{},
		},
	})

	assert.NoError(t, err)
	// Warning should be present AND byte slice pointer should have been
	// modified as well
	assert.Error(t, result.Warning)
	assert.Equal(t, []byte("a"), desc1.Bytes)
}

func mockMmapFdFunc(f mmapFdFuncType) func() {
	old := mmapFdFn
	mmapFdFn = f
	return func() {
		mmapFdFn = old
	}
}
