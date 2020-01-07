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

package close

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTryClose(t *testing.T) {
	tests := []struct {
		input     interface{}
		expectErr bool
	}{
		{
			input:     &closer{returnErr: false},
			expectErr: false,
		},
		{
			input:     &closer{returnErr: true},
			expectErr: true,
		},
		{
			input:     &simpleCloser{},
			expectErr: false,
		},
		{
			input:     &nonCloser{},
			expectErr: true,
		},
	}

	for _, test := range tests {
		err := TryClose(test.input)
		if test.expectErr {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
	}
}

type closer struct {
	returnErr bool
}

func (c *closer) Close() error {
	if c.returnErr {
		return errors.New("")
	}
	return nil
}

type simpleCloser struct{}

func (c *simpleCloser) Close() {}

type nonCloser struct{}
