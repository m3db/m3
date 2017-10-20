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

package config

import (
	"fmt"
	"os"
)

const (
	// DefaultNewFileMode is the default new file mode.
	DefaultNewFileMode = os.FileMode(0666)
	// DefaultNewDirectoryMode is the default new directory mode.
	DefaultNewDirectoryMode = os.FileMode(0755)
)

// FilesystemConfiguration is the filesystem configuration.
type FilesystemConfiguration struct {
	// File path prefix for reading/writing TSDB files
	FilePathPrefix string `yaml:"filePathPrefix" validate:"nonzero"`

	// Write buffer size
	WriteBufferSize int `yaml:"writeBufferSize" validate:"min=1"`

	// Data read buffer size
	DataReadBufferSize int `yaml:"dataReadBufferSize" validate:"min=1"`

	// Info metadata file read buffer size
	InfoReadBufferSize int `yaml:"infoReadBufferSize" validate:"min=1"`

	// Seek data read buffer size
	SeekReadBufferSize int `yaml:"seekReadBufferSize" validate:"min=1"`

	// Disk flush throughput limit in Mb/s
	ThroughputLimitMbps float64 `yaml:"throughputLimitMbps" validate:"min=0.0"`

	// Disk flush throughput check interval
	ThroughputCheckEvery int `yaml:"throughputCheckEvery" validate:"nonzero"`

	// NewFileMode is the new file permissions mode to use when
	// creating files - specify as three digits, e.g. 666.
	NewFileMode *string `yaml:"newFileMode"`

	// NewDirectoryMode is the new file permissions mode to use when
	// creating directories - specify as three digits, e.g. 755.
	NewDirectoryMode *string `yaml:"newDirectoryMode"`
}

// ParseNewFileMode parses the specified new file mode.
func (p FilesystemConfiguration) ParseNewFileMode() (os.FileMode, error) {
	if p.NewFileMode == nil {
		return DefaultNewFileMode, nil
	}

	str := *p.NewFileMode
	if len(str) != 3 {
		return 0, fmt.Errorf("file mode must be 3 chars long")
	}

	str = "0" + str

	var v uint32
	n, err := fmt.Sscanf(str, "%o", &v)
	if err != nil {
		return 0, fmt.Errorf("unable to parse: %v", err)
	}
	if n != 1 {
		return 0, fmt.Errorf("no value to parse")
	}
	return os.FileMode(v), nil
}

// ParseNewDirectoryMode parses the specified new directory mode.
func (p FilesystemConfiguration) ParseNewDirectoryMode() (os.FileMode, error) {
	if p.NewDirectoryMode == nil {
		return DefaultNewDirectoryMode, nil
	}

	str := *p.NewDirectoryMode
	if len(str) != 3 {
		return 0, fmt.Errorf("file mode must be 3 chars long")
	}

	str = "0" + str

	var v uint32
	n, err := fmt.Sscanf(str, "%o", &v)
	if err != nil {
		return 0, fmt.Errorf("unable to parse: %v", err)
	}
	if n != 1 {
		return 0, fmt.Errorf("no value to parse")
	}
	return os.ModeDir | os.FileMode(v), nil
}
