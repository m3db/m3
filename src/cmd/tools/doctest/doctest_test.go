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

package main

import (
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testCaseSource   = "test_case_expected.md.source"
	testCaseScript   = "test_case_expected.sh"
	testCaseMarkdown = "test_case_expected.md"

	testCaseOutputSource   = "test_case_actual.md.source"
	testCaseOutputScript   = "test_case_actual.sh"
	testCaseOutputMarkdown = "test_case_actual.md"
)

func TestDocTest(t *testing.T) {
	testSourceFile, err := ioutil.ReadFile(testCaseSource)
	require.NoError(t, err)

	operationRegEx := regexp.MustCompile(operationRegEx)
	validationRegEx := regexp.MustCompile(validationRegEx)

	// create bash script
	err = bashScript([]byte(testSourceFile), testCaseOutputSource, operationRegEx, validationRegEx)
	require.NoError(t, err)

	expectedScript := readFile(t, testCaseScript)
	actualScript := readFile(t, testCaseOutputScript)

	// create markdown file
	err = markdownFile(testSourceFile, testCaseOutputSource, validationRegEx, operationRegEx)
	require.NoError(t, err)

	actualMarkdown := readFile(t, testCaseOutputMarkdown)
	expectedMarkdown := readFile(t, testCaseMarkdown)

	assert.Equal(t, string(actualScript), string(expectedScript))
	assert.Equal(t, expectedMarkdown, actualMarkdown)

	// Remove script and markdown files
	require.NoError(t, os.Remove(testCaseOutputScript))
	require.NoError(t, os.Remove(testCaseOutputMarkdown))
}

func readFile(t *testing.T, fileName string) []byte {
	f, err := ioutil.ReadFile(fileName)
	require.NoError(t, err)

	return f
}
