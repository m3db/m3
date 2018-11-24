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

package index

import (
	"time"

	"github.com/m3db/m3/src/m3ninx/index/segment"
)

type readableSegment interface {
	Age() time.Duration
}

type mutableReadableSeg struct {
	createdAt time.Time
	segment   segment.MutableSegment
}

var _ readableSegment = &mutableReadableSeg{}

func newMutableReadableSeg(seg segment.MutableSegment) *mutableReadableSeg {
	return &mutableReadableSeg{
		createdAt: time.Now(),
		segment:   seg,
	}
}

func (s *mutableReadableSeg) Segment() segment.MutableSegment {
	return s.segment
}

func (s *mutableReadableSeg) Age() time.Duration {
	return time.Since(s.createdAt)
}

type readableSeg struct {
	createdAt time.Time
	segment   segment.Segment
}

var _ readableSegment = &readableSeg{}

func newReadableSeg(seg segment.Segment) *readableSeg {
	return &readableSeg{
		createdAt: time.Now(),
		segment:   seg,
	}
}

func (s *readableSeg) Segment() segment.Segment {
	return s.segment
}

func (s *readableSeg) Age() time.Duration {
	return time.Since(s.createdAt)
}
