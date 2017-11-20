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

package metric

import (
	"fmt"
	"strings"
)

// Type is a metric type.
type Type int

// A list of supported metric types.
const (
	UnknownType Type = iota
	CounterType
	TimerType
	GaugeType
)

// validTypes is a list of valid types.
var validTypes = []Type{
	CounterType,
	TimerType,
	GaugeType,
}

func (t Type) String() string {
	switch t {
	case CounterType:
		return "counter"
	case TimerType:
		return "timer"
	case GaugeType:
		return "gauge"
	default:
		return "unknown"
	}
}

// ParseType parses a type string and returns the type.
func ParseType(typeStr string) (Type, error) {
	validTypeStrs := make([]string, 0, len(validTypes))
	for _, valid := range validTypes {
		if typeStr == valid.String() {
			return valid, nil
		}
		validTypeStrs = append(validTypeStrs, valid.String())
	}
	return UnknownType, fmt.Errorf("invalid metric type '%s', valid types are: %s",
		typeStr, strings.Join(validTypeStrs, ", "))
}

// MustParseType parses a type string and panics if the input in invalid.
func MustParseType(typeStr string) Type {
	t, err := ParseType(typeStr)
	if err != nil {
		panic(err.Error())
	}
	return t
}

// UnmarshalYAML unmarshals YAML object into a metric type.
func (t *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	mt, err := ParseType(str)
	if err != nil {
		return err
	}

	*t = mt
	return nil
}
