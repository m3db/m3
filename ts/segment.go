// Copyright (c) 2016 Uber Technologies, Inc.
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

package ts

import "github.com/m3db/m3x/pool"

// Segment represents a binary blob consisting of two byte slices, if the
// pools are set for the two byte slices then when the Segment goes out of
// scope the slices should be returned to their respective pools with Finalize.
type Segment struct {
	// Head is the head of the segment.
	Head []byte

	// Tail is the tail of the segment.
	Tail []byte

	// HeadPool if not nil is the bytes pool used to allocate the head.
	HeadPool pool.BytesPool

	// TailPool if not nil is the bytes pool used to allocate the tail.
	TailPool pool.BytesPool
}

// Len returns the length of the head and tail.
func (s Segment) Len() int {
	return len(s.Head) + len(s.Tail)
}

// Finalize will release resources kept by the segment if pooled.
func (s Segment) Finalize() {
	if s.HeadPool != nil && s.Head != nil {
		s.HeadPool.Put(s.Head)
	}
	if s.TailPool != nil && s.Tail != nil {
		s.TailPool.Put(s.Tail)
	}
	s.Head = nil
	s.Tail = nil
}
