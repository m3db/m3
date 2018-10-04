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

package build

import (
	"github.com/m3db/m3/src/m3em/os/fs"
)

type svcBuild struct {
	id         string
	sourcePath string
}

func (b *svcBuild) ID() string {
	return b.id
}

func (b *svcBuild) Iter(bufferSize int) (fs.FileReaderIter, error) {
	return fs.NewSizedFileReaderIter(b.sourcePath, bufferSize)
}

func (b *svcBuild) SourcePath() string {
	return b.sourcePath
}

// NewServiceBuild constructs a new ServiceBuild representing a Service build
func NewServiceBuild(id string, sourcePath string) ServiceBuild {
	return &svcBuild{
		id:         id,
		sourcePath: sourcePath,
	}
}

type svcConfig struct {
	id    string
	bytes []byte
}

func (c *svcConfig) ID() string {
	return c.id
}

func (c *svcConfig) Iter(_ int) (fs.FileReaderIter, error) {
	bytes, err := c.Bytes()
	if err != nil {
		return nil, err
	}
	return fs.NewBytesReaderIter(bytes), nil
}

func (c *svcConfig) Bytes() ([]byte, error) {
	return c.bytes, nil
}

// NewServiceConfig returns a new Service Configuration
func NewServiceConfig(id string, marshalledBytes []byte) ServiceConfiguration {
	return &svcConfig{
		id:    id,
		bytes: marshalledBytes,
	}
}
