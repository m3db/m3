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

package sharding

import (
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestShardSetParseShardSetErrors(t *testing.T) {
	tests := []struct {
		yaml        string
		expectedErr string
	}{
		{yaml: `huh`, expectedErr: "invalid range 'huh'"},
		{yaml: `2..1`, expectedErr: "invalid range: 2 > 1"},
	}

	for _, test := range tests {
		var shards ShardSet
		err := yaml.Unmarshal([]byte(test.yaml), &shards)
		require.Error(t, err)
		require.Equal(t, test.expectedErr, err.Error())
		require.Equal(t, 0, len(shards))
	}
}

func TestShardSetParseShardSet(t *testing.T) {
	tests := []struct {
		yaml     string
		expected []uint32
	}{
		{yaml: `shards: 76`, expected: []uint32{76}},
		{yaml: `shards: [3, 6, 5]`, expected: []uint32{3, 5, 6}},
		{yaml: `shards: ["3"]`, expected: []uint32{3}},
		{yaml: `shards: ["3..8"]`, expected: []uint32{3, 4, 5, 6, 7, 8}},
		{yaml: `shards: ["3", "3..8"]`, expected: []uint32{3, 4, 5, 6, 7, 8}},
		{yaml: `shards: ["3", "3..8", 9]`, expected: []uint32{3, 4, 5, 6, 7, 8, 9}},
		{yaml: `shards: 3`, expected: []uint32{3}},
		{yaml: `shards: "3"`, expected: []uint32{3}},
		{yaml: `shards: "3..8"`, expected: []uint32{3, 4, 5, 6, 7, 8}},
	}

	for i, test := range tests {
		var cfg struct {
			Shards ShardSet
		}

		err := yaml.Unmarshal([]byte(test.yaml), &cfg)
		require.NoError(t, err, "received error for test %d", i)
		ValidateShardSet(t, test.expected, cfg.Shards)
	}
}

func TestParseShardSet(t *testing.T) {
	tests := []struct {
		str      string
		expected []uint32
	}{
		{str: `76`, expected: []uint32{76}},
		{str: `3..8`, expected: []uint32{3, 4, 5, 6, 7, 8}},
		{str: `3..3`, expected: []uint32{3}},
	}

	for _, test := range tests {
		parsed, err := ParseShardSet(test.str)
		require.NoError(t, err)
		ValidateShardSet(t, test.expected, parsed)
	}
}

func TestParseShardSetErrors(t *testing.T) {
	tests := []string{
		`huh`,
		`76..`,
		`2..1`,
	}

	for _, test := range tests {
		_, err := ParseShardSet(test)
		require.Error(t, err)
	}
}

func TestMustParseShardSet(t *testing.T) {
	tests := []struct {
		str      string
		expected []uint32
	}{
		{str: `76`, expected: []uint32{76}},
		{str: `3..8`, expected: []uint32{3, 4, 5, 6, 7, 8}},
		{str: `3..3`, expected: []uint32{3}},
	}

	for _, test := range tests {
		parsed := MustParseShardSet(test.str)
		ValidateShardSet(t, test.expected, parsed)
	}
}

func TestMustParseShardSetPanics(t *testing.T) {
	tests := []string{
		`huh`,
		`76..`,
		`2..1`,
	}

	for _, test := range tests {
		require.Panics(t, func() { MustParseShardSet(test) })
	}
}
