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

package config

import "fmt"

// StringSlice represents a slice of strings. When used as a flag variable,
// it allows for multiple string values. For example, it can be used like this:
// 	var configFiles StringSlice
// 	flag.Var(&configFiles, "f", "configuration file(s)")
// Then it can be invoked like this:
// 	./app -f file1.yaml -f file2.yaml -f valueN.yaml
// Finally, when the flags are parsed, the variable contains all the values.
type StringSlice []string

// String() returns a string implmentation of the slice.
func (i *StringSlice) String() string {
	if i == nil {
		return ""
	}
	return fmt.Sprintf("%v", ([]string)(*i))
}

// Set appends a string value to the slice.
func (i *StringSlice) Set(value string) error {
	*i = append(*i, value)
	return nil
}
