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

package buffer

import (
	"testing"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestStrategyYamlUnmarshal(t *testing.T) {
	var cfg OnFullStrategy
	tests := []struct {
		bytes            []byte
		expectErr        bool
		expectedStrategy OnFullStrategy
	}{
		{
			bytes:            []byte("dropEarliest"),
			expectErr:        false,
			expectedStrategy: DropEarliest,
		},
		{
			bytes:            []byte("returnError"),
			expectErr:        false,
			expectedStrategy: ReturnError,
		},
		{
			bytes:     []byte("bad"),
			expectErr: true,
		},
	}
	for _, test := range tests {
		err := yaml.Unmarshal(test.bytes, &cfg)
		if test.expectErr {
			require.Error(t, err)
			continue
		}
		require.Equal(t, test.expectedStrategy, cfg)
	}
}
