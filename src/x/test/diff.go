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

package test

import (
	"encoding/json"

	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/stretchr/testify/require"

	xjson "github.com/m3db/m3/src/x/json"
)

// Diff is a helper method to print a terminal pretty diff of two strings
// for test output purposes.
func Diff(expected, actual string) string {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(expected, actual, false)
	return dmp.DiffPrettyText(diffs)
}

// MustPrettyJSONMap returns an indented JSON string of the object.
func MustPrettyJSONMap(t require.TestingT, value xjson.Map) string {
	pretty, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	return string(pretty)
}

// MustPrettyJSONArray returns an indented JSON string of the object.
func MustPrettyJSONArray(t require.TestingT, value xjson.Array) string {
	pretty, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	return string(pretty)
}

// MustPrettyJSONObject returns an indented JSON string of the object.
func MustPrettyJSONObject(t require.TestingT, value interface{}) string {
	pretty, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	return string(pretty)
}

// MustPrettyJSONString returns an indented version of the JSON.
func MustPrettyJSONString(t require.TestingT, str string) string {
	var unmarshalled map[string]interface{}
	err := json.Unmarshal([]byte(str), &unmarshalled)
	require.NoError(t, err)
	pretty, err := json.MarshalIndent(unmarshalled, "", "  ")
	require.NoError(t, err)
	return string(pretty)
}
