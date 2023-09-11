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

package graphite

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobToRegexPattern(t *testing.T) {
	tests := []struct {
		glob    string
		isRegex bool
		regex   string
		opts    GlobOptions
	}{
		{
			glob:    "barbaz",
			isRegex: false,
			regex:   "barbaz",
		},
		{
			glob:    "barbaz:quxqaz",
			isRegex: false,
			regex:   "barbaz:quxqaz",
		},
		{
			glob:    "bar:baz~",
			isRegex: false,
			regex:   "bar:baz~",
		},
		{
			glob:    "foo\\+bar.'baz<1001>'.qux",
			isRegex: true,
			regex:   "foo\\+bar\\.+\\'baz\\<1001\\>\\'\\.+qux",
		},
		{
			glob:    "foo.host.me{1,2,3}.*",
			isRegex: true,
			regex:   "foo\\.+host\\.+me(1|2|3)\\.+[^\\.]*",
		},
		{
			glob:    "bar.zed.whatever[0-9].*.*.bar",
			isRegex: true,
			regex:   "bar\\.+zed\\.+whatever[0-9]\\.+[^\\.]*\\.+[^\\.]*\\.+bar",
		},
		{
			glob:    "foo{0[3-9],1[0-9],20}",
			isRegex: true,
			regex:   "foo(0[3-9]|1[0-9]|20)",
		},
		{
			glob:    "foo{0[3-9],1[0-9],20}:bar",
			isRegex: true,
			regex:   "foo(0[3-9]|1[0-9]|20):bar",
		},
		{
			glob:    "foo.**.bar.baz",
			isRegex: true,
			regex:   "foo\\.+.*bar\\.+baz",
			opts:    GlobOptions{AllowMatchAll: true},
		},
		{
			glob:    "foo**bar.baz",
			isRegex: true,
			regex:   "foo.*bar\\.+baz",
			opts:    GlobOptions{AllowMatchAll: true},
		},
	}

	for _, test := range tests {
		pattern, isRegex, err := ExtendedGlobToRegexPattern(test.glob, test.opts)
		require.NoError(t, err)
		assert.Equal(t, test.isRegex, isRegex)
		assert.Equal(t, test.regex, string(pattern), "bad pattern for %s", test.glob)
	}
}

func TestGlobToRegexPatternErrors(t *testing.T) {
	tests := []struct {
		glob string
		err  string
	}{
		{"foo.host{1,2", "unbalanced '{' in foo.host{1,2"},
		{"foo.host{1,2]", "invalid ']' at 12, no prior for '[' in foo.host{1,2]"},
		{"foo.,", "invalid ',' outside of matching group at pos 4 in foo.,"},
		{"foo.host{a[0-}", "invalid '}' at 13, no prior for '{' in foo.host{a[0-}"},
	}

	for _, test := range tests {
		_, _, err := GlobToRegexPattern(test.glob)
		require.Error(t, err)
		assert.Equal(t, test.err, err.Error(), "invalid error for %s", test.glob)
	}
}

func TestCompileGlob(t *testing.T) {
	tests := []struct {
		glob    string
		match   bool
		toMatch []string
	}{
		{"foo.bar.timers.baz??-bar.qux.query.count", true,
			[]string{
				"foo.bar.timers.baz01-bar.qux.query.count",
				"foo.bar.timers.baz24-bar.qux.query.count"}},
		{"foo.bar.timers.baz??-bar.qux.query.count", false,
			[]string{
				"foo.bar.timers.baz-bar.qux.query.count",
				"foo.bar.timers.baz.0-bar.qux.query.count",
				"foo.bar.timers.baz021-bar.qux.query.count",
				"foo.bar.timers.baz991-bar.qux.query.count"}},
		{"foo.host{1,2}.*", true,
			[]string{"foo.host1.zed", "foo.host2.whatever"}},
		{"foo.*.zed.*", true,
			[]string{"foo.bar.zed.eq", "foo.zed.zed.zed"}},
		{"foo.*.zed.*", false,
			[]string{"bar.bar.zed.zed", "foo.bar.zed", "foo.bar.zed.eq.monk"}},
		{"foo.host{1,2}.zed", true,
			[]string{"foo.host1.zed", "foo.host2.zed"}},
		{"foo.host{1,2}.zed", false,
			[]string{"foo.host3.zed", "foo.hostA.zed", "blad.host1.zed", "foo.host1.zed.z"}},
		{"optic{0[3-9],1[0-9],20}", true,
			[]string{"optic03", "optic10", "optic20"}},
		{"optic{0[3-9],1[0-9],20}", false,
			[]string{"optic01", "optic21", "optic201", "optic031"}},
	}

	for _, test := range tests {
		rePattern, _, err := GlobToRegexPattern(test.glob)
		require.NoError(t, err)
		re := regexp.MustCompile(fmt.Sprintf("^%s$", rePattern))
		for _, s := range test.toMatch {
			matched := re.MatchString(s)
			assert.Equal(t, test.match, matched, "incorrect match between %s and %s", test.glob, s)
		}
	}
}
