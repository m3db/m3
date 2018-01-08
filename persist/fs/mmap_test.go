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

package fs

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestMmapFile(t *testing.T) {
	fd, err := ioutil.TempFile("", "testfile")
	assert.NoError(t, err)

	result, err := mmapFile(fd, mmapOptions{})
	assert.NoError(t, err)
	assert.NoError(t, result.warning)
	assert.Equal(t, []byte{}, result.result)

	munmap(result.result)
}

func TestMmapFiles(t *testing.T) {
	fd1, err := ioutil.TempFile("", "1")
	assert.NoError(t, err)
	fd1Path := fd1.Name()
	fd2, err := ioutil.TempFile("", "2")
	assert.NoError(t, err)
	fd2Path := fd2.Name()

	var (
		bytes1 = []byte{}
		bytes2 = []byte{}
	)
	result, err := mmapFiles(os.Open, map[string]mmapFileDesc{
		fd1Path: mmapFileDesc{
			file:    &fd1,
			bytes:   &bytes1,
			options: mmapOptions{},
		},
		fd2Path: mmapFileDesc{
			file:    &fd2,
			bytes:   &bytes2,
			options: mmapOptions{},
		},
	})

	assert.NoError(t, err)
	assert.NoError(t, result.warning)
}

func TestMmapFilesHandlesError(t *testing.T) {
	fd1, err := ioutil.TempFile("", "1")
	assert.NoError(t, err)
	fd1Path := fd1.Name()

	fd2, err := ioutil.TempFile("", "doesnt-matter")
	assert.NoError(t, err)
	var (
		bytes1 = []byte{}
		bytes2 = []byte{}
	)
	_, err = mmapFiles(os.Open, map[string]mmapFileDesc{
		fd1Path: mmapFileDesc{
			file:    &fd1,
			bytes:   &bytes1,
			options: mmapOptions{},
		},
		"does_not_exist": mmapFileDesc{
			file:    &fd2,
			bytes:   &bytes2,
			options: mmapOptions{},
		},
	})

	assert.Error(t, err)
}

func TestMmapFilesHandlesWarnings(t *testing.T) {
	fd1, err := ioutil.TempFile("", "1")
	assert.NoError(t, err)
	fd1Path := fd1.Name()

	bytes1 := []byte{}

	warningFunc := func(fd, offset, length int64, opts mmapOptions) (mmapResult, error) {
		return mmapResult{warning: errors.New("some-error"), result: []byte("a")}, nil
	}
	result, err := mmapFilesWithFunc(os.Open, map[string]mmapFileDesc{
		fd1Path: mmapFileDesc{
			file:    &fd1,
			bytes:   &bytes1,
			options: mmapOptions{},
		},
	}, warningFunc)

	assert.NoError(t, err)
	// Warning should be present AND byte slice pointer should have been
	// modified as well
	assert.Error(t, result.warning)
	assert.Equal(t, []byte("a"), bytes1)
}
