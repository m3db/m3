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

package storage

import (
	"fmt"
)

// DataMode is the data storage mode.
type DataMode uint

const (
	// DataModeFloats indicates that only floats will be stored.
	DataModeFloats DataMode = iota
	// DataModeProtoBuf indicates that Protobufs will be stored.
	DataModeProtoBuf

	// DefaultDataMode is the default data mode.
	DefaultDataMode = DataModeFloats
)

// ValidDataModes returns the valid data modes.
func ValidDataModes() []DataMode {
	return []DataMode{DataModeFloats, DataModeProtoBuf}
}

func (p DataMode) String() string {
	switch p {
	case DataModeFloats:
		return "floats"
	case DataModeProtoBuf:
		return "protobuf"
	default:
		return "unknown"
	}
}

// ValidateDataMode validates a data mode.
func ValidateDataMode(v DataMode) error {
	validDataMode := false
	for _, valid := range ValidDataModes() {
		if valid == v {
			validDataMode = true
			break
		}
	}

	if !validDataMode {
		return fmt.Errorf("invalid data mode '%d' valid types are: %v",
			uint(v), ValidDataModes())
	}
	return nil
}

// ParseDataMode parses a DataMode from a string.
func ParseDataMode(str string) (DataMode, error) {
	var r DataMode
	if str == "" {
		return DefaultDataMode, nil
	}

	for _, valid := range ValidDataModes() {
		if str == valid.String() {
			r = valid
			return r, nil
		}
	}
	return r, fmt.Errorf("invalid series DataMode '%s' valid types are: %v",
		str, ValidDataModes())
}

// UnmarshalYAML unmarshals an DataMode into a valid type from string.
func (p *DataMode) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	r, err := ParseDataMode(str)
	if err != nil {
		return err
	}

	*p = r
	return nil
}
