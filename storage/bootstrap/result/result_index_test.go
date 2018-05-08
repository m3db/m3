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

package result

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3ninx/index/segment"

	"github.com/stretchr/testify/assert"
)

func TestIndexResultAddMergesExistingSegments(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(testBlockSize)

	segments := []segment.Segment{
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
		segment.NewMockSegment(ctrl),
	}

	times := []time.Time{start, start.Add(testBlockSize)}

	r := NewIndexBootstrapResult()
	r.Add(NewIndexBlock(times[0], []segment.Segment{segments[0]}), nil)
	r.Add(NewIndexBlock(times[0], []segment.Segment{segments[1]}), nil)
	r.Add(NewIndexBlock(times[1], []segment.Segment{segments[2], segments[3]}), nil)

	merged := NewIndexBootstrapResult()
	merged.Add(NewIndexBlock(times[0], []segment.Segment{segments[4]}), nil)
	merged.Add(NewIndexBlock(times[1], []segment.Segment{segments[5]}), nil)

	expected := NewIndexBootstrapResult()
	expected.Add(NewIndexBlock(times[0], []segment.Segment{segments[0], segments[1], segments[4]}), nil)
	expected.Add(NewIndexBlock(times[1], []segment.Segment{segments[2], segments[3], segments[5]}), nil)

	assert.True(t, segmentsInResultsSame(expected.IndexResults(), merged.IndexResults()))
}

func segmentsInResultsSame(a, b IndexResults) bool {
	if len(a) != len(b) {
		// ``
		fmt.Printf("not same len\n")
		return false
	}
	for t, block := range a {
		otherBlock, ok := b[t]
		if !ok {
			fmt.Printf("no block at time %v\n", time.Unix(0, int64(t)).String())
			return false
		}
		if len(block.Segments()) != len(otherBlock.Segments()) {
			fmt.Printf("block segment len not match at %v\n", time.Unix(0, int64(t)).String())
			return false
		}
		for i, s := range block.Segments() {
			if s != otherBlock.Segments()[i] {
				fmt.Printf("block segment not match at %v idx %d\n", time.Unix(0, int64(t)).String(), i)
				return false
			}
		}
	}
	return true
}
