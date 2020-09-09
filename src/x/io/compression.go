// Copyright (c) 2020 Uber Technologies, Inc.
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

package io

import (
	"fmt"
	"io"

	snappy "github.com/golang/snappy"
)

// CompressionMethod used for the read/write buffer
type CompressionMethod byte

var validCompressionMethods = []CompressionMethod{
	NoCompression,
	SnappyCompression,
}

const (
	// NoCompression means compression is disabled for the read/write buffer
	NoCompression CompressionMethod = iota

	// SnappyCompression enables snappy compression for read/write buffer
	SnappyCompression
)

func (cm CompressionMethod) String() string {
	switch cm {
	case NoCompression:
		return "none"
	case SnappyCompression:
		return "snappy"
	default:
		return ""
	}
}

// UnmarshalYAML unmarshals compression method from YAML configuration
func (cm *CompressionMethod) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	for _, valid := range validCompressionMethods {
		if str == valid.String() {
			*cm = valid
			return nil
		}
	}

	return fmt.Errorf("invalid CompressionMethod '%s' valid types are: %v", str, validCompressionMethods)
}

// SnappyResettableWriterFn returns a snappy compression enabled writer
func SnappyResettableWriterFn() ResettableWriterFn {
	return func(r io.Writer, opts ResettableWriterOptions) ResettableWriter {
		return snappy.NewBufferedWriter(r)
	}
}

// SnappyResettableReaderFn returns a snappy compression enabled reader
func SnappyResettableReaderFn() ResettableReaderFn {
	return func(r io.Reader, opts ResettableReaderOptions) ResettableReader {
		return snappy.NewReader(r)
	}
}
