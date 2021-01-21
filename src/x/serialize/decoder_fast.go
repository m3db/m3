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

package serialize

import (
	"bytes"
	"fmt"
)

// TagValueFromEncodedTagsFast returns a tag from a set of encoded tags without
// any pooling required.
func TagValueFromEncodedTagsFast(
	encodedTags []byte,
	tagName []byte,
) ([]byte, bool, error) {
	total := len(encodedTags)
	if total < 4 {
		return nil, false, fmt.Errorf(
			"encoded tags too short: size=%d, need=%d", total, 4)
	}

	header := ByteOrder.Uint16(encodedTags[:2])
	encodedTags = encodedTags[2:]
	if header != HeaderMagicNumber {
		return nil, false, ErrIncorrectHeader
	}

	length := int(ByteOrder.Uint16(encodedTags[:2]))
	encodedTags = encodedTags[2:]

	for i := 0; i < length; i++ {
		if len(encodedTags) < 2 {
			return nil, false, fmt.Errorf("missing size for tag name: index=%d", i)
		}
		numBytesName := int(ByteOrder.Uint16(encodedTags[:2]))
		if numBytesName == 0 {
			return nil, false, ErrEmptyTagNameLiteral
		}
		encodedTags = encodedTags[2:]

		bytesName := encodedTags[:numBytesName]
		encodedTags = encodedTags[numBytesName:]

		if len(encodedTags) < 2 {
			return nil, false, fmt.Errorf("missing size for tag value: index=%d", i)
		}

		numBytesValue := int(ByteOrder.Uint16(encodedTags[:2]))
		encodedTags = encodedTags[2:]

		bytesValue := encodedTags[:numBytesValue]
		encodedTags = encodedTags[numBytesValue:]

		if bytes.Equal(bytesName, tagName) {
			return bytesValue, true, nil
		}
	}

	return nil, false, nil
}
