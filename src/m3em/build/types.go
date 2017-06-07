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
	"github.com/m3db/m3em/os/fs"
)

// IterableBytesWithID represents the an iterable byte stream associated
// with an identifier
type IterableBytesWithID interface {
	// ID returns a string identifier for the build
	ID() string

	// Iter returns an iterator to loop over the contents of the build
	Iter(bufferSize int) (fs.FileReaderIter, error)
}

// ServiceBuild represents a single build of a service, available locally.
type ServiceBuild interface {
	IterableBytesWithID

	// SourcePath returns a local path where the service build is available
	SourcePath() string
}

// ServiceConfiguration represents the configuration required to operate a service
type ServiceConfiguration interface {
	IterableBytesWithID

	// Bytes returns a UTF-8 serialized representation of the service configuration
	Bytes() ([]byte, error)
}
