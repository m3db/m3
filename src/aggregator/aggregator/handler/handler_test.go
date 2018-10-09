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
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestTypeUnmarshalYAML(t *testing.T) {
	inputs := []struct {
		str      string
		expected Type
	}{
		{str: "blackhole", expected: blackholeType},
		{str: "logging", expected: loggingType},
		{str: "forward", expected: forwardType},
	}
	for _, input := range inputs {
		var typ Type
		require.NoError(t, yaml.Unmarshal([]byte(input.str), &typ))
		require.Equal(t, input.expected, typ)
	}
}

func TestTypeUnmarshalYAMLErrors(t *testing.T) {
	var typ Type
	err := yaml.Unmarshal([]byte("huh"), &typ)
	require.Error(t, err)
	require.Equal(t, "invalid handler type 'huh' valid types are: blackhole, logging, forward", err.Error())
}
