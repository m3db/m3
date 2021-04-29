// Copyright (c) 2021 Uber Technologies, Inc.
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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
)

// FakeTagEncoder is a fake TagEncoder for testing.
// It encodes a set of TagPair as name1#value1#name2#value#...
type FakeTagEncoder struct {
	data []byte
}

// Decode the encoded data back into a set of TagPairs.
func (f *FakeTagEncoder) Decode() []id.TagPair {
	tags := make([]id.TagPair, 0)
	parts := bytes.Split(f.data[0:len(f.data)-1], []byte{'#'})
	for i := 0; i < len(parts); i += 2 {
		tags = append(tags, id.TagPair{
			Name:  parts[i],
			Value: parts[i+1],
		})
	}
	return tags
}

// Encode the tags. The original tags can be retrieved with Decode for testing.
func (f *FakeTagEncoder) Encode(tags ident.TagIterator) error {
	if len(f.data) > 0 {
		return errors.New("must call Reset if reusing the fake encoder")
	}
	for tags.Next() {
		cur := tags.Current()
		if err := checkValue(cur.Name.Bytes()); err != nil {
			return err
		}
		if err := checkValue(cur.Value.Bytes()); err != nil {
			return err
		}
		f.data = append(f.data, cur.Name.Bytes()...)
		f.data = append(f.data, '#')
		f.data = append(f.data, cur.Value.Bytes()...)
		f.data = append(f.data, '#')
	}
	return nil
}

func checkValue(v []byte) error {
	if bytes.Contains(v, []byte{'#'}) {
		return fmt.Errorf("%v cannot contain a #", v)
	}
	return nil
}

// Data gets the encoded tags.
func (f *FakeTagEncoder) Data() (checked.Bytes, bool) {
	b := checked.NewBytes(f.data, nil)
	b.IncRef()
	return b, true
}

// Reset the stored encoded data.
func (f *FakeTagEncoder) Reset() {
	f.data = f.data[:0]
}

// Finalize does nothing.
func (f *FakeTagEncoder) Finalize() {
}

var _ TagEncoder = &FakeTagEncoder{}
