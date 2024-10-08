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

type uncheckedMetricTagsIter struct {
	data    []byte
	decoder *uncheckedDecoder
}

// NewUncheckedMetricTagsIterator creates a MetricTagsIterator that removes all safety checks (i.e ref counts).
// It is suitable to use this when you are confident the provided bytes to iterate are not shared.
func NewUncheckedMetricTagsIterator(tagLimits TagSerializationLimits) MetricTagsIterator {
	return &uncheckedMetricTagsIter{
		decoder: newUncheckedTagDecoder(tagLimits),
	}
}

func (it *uncheckedMetricTagsIter) Reset(sortedTagPairs []byte) {
	it.data = sortedTagPairs
	it.decoder.reset(sortedTagPairs)
}

func (it *uncheckedMetricTagsIter) Bytes() []byte {
	return it.data
}

func (it *uncheckedMetricTagsIter) NumTags() int {
	return it.decoder.length
}

func (it *uncheckedMetricTagsIter) TagValue(tagName []byte) ([]byte, bool) {
	// use the fast decoder (i.e no pools) since this lookup is outside the current iteration.
	// decoding errors are dropped and the tag is reported as missing.
	value, found, _ := TagValueFromEncodedTagsFast(it.data, tagName)
	return value, found
}

func (it *uncheckedMetricTagsIter) Next() bool {
	return it.decoder.next()
}

func (it *uncheckedMetricTagsIter) Current() ([]byte, []byte) {
	return it.decoder.current()
}

func (it *uncheckedMetricTagsIter) Err() error {
	return it.decoder.err
}

func (it *uncheckedMetricTagsIter) Close() {
	panic("should not close")
}
