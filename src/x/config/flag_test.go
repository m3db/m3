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

package config_test

import (
	"flag"
	"testing"

	"github.com/m3db/m3/src/x/config"

	"github.com/stretchr/testify/assert"
)

func TestFlagArray(t *testing.T) {
	tests := []struct {
		args []string
		name string
		want string
	}{
		{
			name: "single value",
			args: []string{"-f", "./some/file/path/here.yaml"},
			want: "[./some/file/path/here.yaml]",
		},
		{
			name: "single empty value",
			args: []string{"-f", ""},
			want: "[]",
		},
		{
			name: "two value",
			args: []string{"-f", "file1.yaml", "-f", "file2.yaml"},
			want: "[file1.yaml file2.yaml]",
		},
		{
			name: "two value one of which empty",
			args: []string{"-f", "", "-f", "file2.yaml"},
			want: "[ file2.yaml]",
		},
		{
			name: "three values",
			args: []string{"-f", "file1.yaml", "-f", "file2.yaml", "-f", "file3.yaml"},
			want: "[file1.yaml file2.yaml file3.yaml]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var values config.FlagStringSlice
			fs := flag.NewFlagSet(tt.name, flag.PanicOnError)
			fs.Var(&values, "f", "config files")
			err := fs.Parse(tt.args)
			assert.NoError(t, err, "error parsing flags")
			assert.Equal(t, tt.want, values.String(), "unexpected output")
		})
	}
}

func TestFlagStringSliceNilFlag(t *testing.T) {
	var s *config.FlagStringSlice
	assert.Equal(t, "", s.String(), "nil string slice representation")
}

func TestFlagStringSliceWithOtherFlags(t *testing.T) {
	var values config.FlagStringSlice
	var x string
	fs := flag.NewFlagSet("app", flag.PanicOnError)
	fs.StringVar(&x, "x", "", "some random var")
	fs.Var(&values, "f", "config files")
	fs.Parse([]string{"-f", "file1.yaml", "-x", "file2.yaml", "-f", "file3.yaml"})
	assert.Equal(t, "[file1.yaml file3.yaml]", values.String(), "flag string slice representation")
	assert.Equal(t, "file2.yaml", x, "x value")
}
