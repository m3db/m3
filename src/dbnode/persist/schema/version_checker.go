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

package schema

// VersionChecker centralizes logic for checking if a major, minor version combo supports
// specific functionality
type VersionChecker struct {
	majorVersion int
	minorVersion int
}

// NewVersionChecker creates a new VersionChecker
func NewVersionChecker(majorVersion int, minorVersion int) VersionChecker {
	return VersionChecker{
		majorVersion: majorVersion,
		minorVersion: minorVersion,
	}
}

// IndexEntryValidationEnabled checks the version to determine if
// fileset files of the specified version allow for doing checksum validation
// on individual index entries
func (v *VersionChecker) IndexEntryValidationEnabled() bool {
	return v.majorVersion >= 2 || v.majorVersion == 1 && v.minorVersion >= 1
}
