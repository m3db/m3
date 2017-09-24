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

package handler

import (
	"fmt"
	"strings"
)

// Type is the handler type.
type Type string

// A list of supported handler types.
const (
	blackholeType Type = "blackhole"
	loggingType   Type = "logging"
	forwardType   Type = "forward"
)

var (
	validHandlerTypes = []Type{
		blackholeType,
		loggingType,
		forwardType,
	}
)

// UnmarshalYAML unmarshals YAML into a type.
func (t *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	validTypes := make([]string, 0, len(validHandlerTypes))
	for _, valid := range validHandlerTypes {
		if str == string(valid) {
			*t = valid
			return nil
		}
		validTypes = append(validTypes, string(valid))
	}
	return fmt.Errorf("invalid handler type '%s' valid types are: %s",
		str, strings.Join(validTypes, ", "))
}
