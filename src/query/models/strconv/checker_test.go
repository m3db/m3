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

package strconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func generateUnescapedSlice() []byte {
	bottomBound := int(' ')
	upperBound := int('~')
	unescaped := make([]byte, upperBound-bottomBound)
	ignore := int('"')
	idx := 0
	for i := bottomBound; i <= upperBound; i++ {
		if i != ignore {
			unescaped[idx] = byte(i)
			idx++
		}
	}

	return unescaped
}

func TestUnescapedSliceDoesNotNeedToEscape(t *testing.T) {
	unescaped := generateUnescapedSlice()
	assert.False(t, NeedToEscape(unescaped))
}

func TestSliceWithQuoteNeedsToEscape(t *testing.T) {
	unescaped := generateUnescapedSlice()
	unescaped = append(unescaped, '"')
	assert.True(t, NeedToEscape(unescaped))
}

func TestSliceWithControlCharactersNeedsToEscape(t *testing.T) {
	unescaped := generateUnescapedSlice()
	lowByte := byte(int(' ') - 1)
	unescapedWithLowByte := append(unescaped, lowByte)
	assert.True(t, NeedToEscape(unescapedWithLowByte))

	highByte := byte(int('~') + 1)
	unescaped = append(unescaped, highByte)
	assert.True(t, NeedToEscape(unescaped))
}
