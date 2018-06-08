// Copyright (c) 2018 Uber Technologies, Inc.
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

package bytes

// ArraysFromStringArray converts a string array to a byte slice array.
func ArraysFromStringArray(values []string) [][]byte {
	result := make([][]byte, len(values))
	for i, str := range values {
		result[i] = []byte(str)
	}
	return result
}

// ArraysToStringArray converts a byte slice array to a string array.
func ArraysToStringArray(values [][]byte) []string {
	result := make([]string, len(values))
	for i, bytes := range values {
		result[i] = string(bytes)
	}
	return result
}

// ArrayCopy copies a byte slice array to a new byte slice array.
func ArrayCopy(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i, b := range values {
		result[i] = make([]byte, len(b))
		copy(result[i], b)
	}
	return result
}
