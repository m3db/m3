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

package composite

import (
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	xerrors "github.com/m3db/m3x/errors"
)

var (
	errSegmentClosed = errors.New("unable to perform operation, segment is closed")
)

// compositeSegment abstracts multiple segments into a single segment. It relies
// on the ordered iteration provided by FSTs to reduce the memory required to
// afford such an abstraction.

// NewSegment returns a new composite segment backed by the provided segments.
func NewSegment(opts Options, segments ...sgmt.Segment) (sgmt.Segment, error) {
	success := false
	subs := make([]*subSegment, 0, len(segments))
	defer func() {
		if success {
			return
		}
		for _, s := range subs {
			s.Close()
		}
	}()

	// The doc ID transformation looks as follows -
	// input:
	//                size   min    max
	// subSegment-1   10     10     20
	// subSegment-2   100    1      101
	// sugSegment-3   50     1      51
	//
	// output:
	//                size   min    max
	// composite      160    1      161
	// [1,11)    <- subSegment-1
	// [11,111)  <- subSegment-2
	// [111,161) <- subSegment-3
	var (
		externalStartInclusive = postings.ID(0)
		currentStartInclusive  = externalStartInclusive
		externalEndExclusive   = externalStartInclusive + 1
	)
	for _, s := range segments {
		r, err := s.Reader()
		if err != nil {
			return nil, err
		}
		internalStartInclusive, internalEndExclusive := r.DocRange()
		externalEndExclusive = currentStartInclusive + (internalEndExclusive - internalStartInclusive)
		subs = append(subs, &subSegment{
			seg:    s,
			reader: r,
			internalLimits: segmentLimits{
				startInclusive: internalStartInclusive,
				endExclusive:   internalEndExclusive,
			},
			externalLimits: segmentLimits{
				startInclusive: currentStartInclusive,
				endExclusive:   externalEndExclusive,
			},
		})
		currentStartInclusive = externalEndExclusive
	}

	success = true
	return &compositeSegment{
		opts: opts,
		subs: subs,
		size: int64(externalEndExclusive - externalStartInclusive),
		limits: segmentLimits{
			startInclusive: externalStartInclusive,
			endExclusive:   externalEndExclusive,
		},
	}, nil
}

type compositeSegment struct {
	sync.RWMutex
	closed bool

	opts   Options
	subs   []*subSegment
	size   int64
	limits segmentLimits
}

var _ sgmt.Segment = &compositeSegment{}
var _ index.Reader = &compositeSegment{}

func (c *compositeSegment) Size() int64 {
	c.RLock()
	defer c.RUnlock()
	return c.size
}

func (c *compositeSegment) ContainsID(docID []byte) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return false, errSegmentClosed
	}
	for _, s := range c.subs {
		contains, err := s.seg.ContainsID(docID)
		if err != nil {
			return false, err
		}
		if contains {
			return true, nil
		}
	}
	return false, nil
}

func (c *compositeSegment) Reader() (index.Reader, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return nil, errSegmentClosed
	}
	return c, nil
}

func (c *compositeSegment) Fields() (sgmt.FieldsIterator, error) {
	return c.newMergeIter(func(s *subSegment) (sgmt.OrderedBytesIterator, error) {
		return s.seg.Fields()
	})
}

func (c *compositeSegment) Terms(field []byte) (sgmt.TermsIterator, error) {
	return c.newMergeIter(func(s *subSegment) (sgmt.OrderedBytesIterator, error) {
		return s.seg.Terms(field)
	})
}

type orderedIterFn func(s *subSegment) (sgmt.OrderedBytesIterator, error)

func (c *compositeSegment) newMergeIter(fn orderedIterFn) (sgmt.TermsIterator, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return nil, errSegmentClosed
	}

	success := false
	iters := make([]sgmt.OrderedBytesIterator, 0, len(c.subs))
	defer func() {
		if !success {
			for _, iter := range iters {
				iter.Close()
			}
		}
	}()

	for _, sub := range c.subs {
		iter, err := fn(sub)
		if err != nil {
			return nil, err
		}
		iters = append(iters, iter)
	}

	success = true
	return newMergedOrderedBytesIterator(iters...), nil
}

func (c *compositeSegment) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errSegmentClosed
	}
	c.closed = true
	var multiErr xerrors.MultiError
	for _, s := range c.subs {
		multiErr = multiErr.Add(s.Close())
	}
	c.subs = nil
	return multiErr.FinalError()
}

func (c *compositeSegment) DocRange() (start, end postings.ID) {
	return c.limits.startInclusive, c.limits.endExclusive
}

func (c *compositeSegment) Doc(id postings.ID) (doc.Document, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return doc.Document{}, errSegmentClosed
	}
	idx, err := c.findSubForIDWithRLock(id)
	if err != nil {
		return doc.Document{}, err
	}
	sub := c.subs[idx]
	return sub.Doc(id)
}

func (c *compositeSegment) MatchTerm(
	field []byte,
	term []byte,
) (postings.List, error) {
	return c.mergePostingsLists(func(s *subSegment) (postings.List, error) {
		return s.reader.MatchTerm(field, term)
	})
}

func (c *compositeSegment) MatchRegexp(
	field []byte,
	regexp []byte,
	compiled index.CompiledRegex,
) (postings.List, error) {
	return c.mergePostingsLists(func(s *subSegment) (postings.List, error) {
		return s.reader.MatchRegexp(field, regexp, compiled)
	})
}

type fetchPostingsListFn func(s *subSegment) (postings.List, error)

func (c *compositeSegment) mergePostingsLists(fn fetchPostingsListFn) (postings.List, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return nil, errSegmentClosed
	}
	pl := c.opts.PostingsListPool().Get()
	for _, sub := range c.subs {
		subPl, err := fn(sub)
		if err != nil {
			return nil, err
		}

		if sub.internalLimits == sub.externalLimits {
			if err := pl.Union(subPl); err != nil {
				return nil, err
			}
			continue
		}

		iter := subPl.Iterator()
		for iter.Next() {
			id := iter.Current()
			transposedID := (id - sub.internalLimits.startInclusive) + sub.externalLimits.startInclusive
			pl.Insert(transposedID)
		}

		if err := iter.Err(); err != nil {
			return nil, err
		}

		if err := iter.Close(); err != nil {
			return nil, err
		}
	}
	return pl, nil
}

func (c *compositeSegment) MatchAll() (postings.MutableList, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return nil, errSegmentClosed
	}
	pl := c.opts.PostingsListPool().Get()
	pl.AddRange(c.limits.startInclusive, c.limits.endExclusive)
	return pl, nil
}

func (c *compositeSegment) Docs(pl postings.List) (doc.Iterator, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return nil, errSegmentClosed
	}
	return index.NewIDDocIterator(c, pl.Iterator()), nil
}

func (c *compositeSegment) AllDocs() (index.IDDocIterator, error) {
	c.RLock()
	defer c.RUnlock()
	if c.closed {
		return nil, errSegmentClosed
	}
	pi := postings.NewRangeIterator(c.limits.startInclusive, c.limits.endExclusive)
	return index.NewIDDocIterator(c, pi), nil
}

func (c *compositeSegment) findSubForIDWithRLock(id postings.ID) (int, error) {
	for idx, s := range c.subs {
		l := s.externalLimits
		if l.startInclusive <= id && id < l.endExclusive {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("unknown id: %d", id)
}

type subSegment struct {
	seg    sgmt.Segment
	reader index.Reader
	// internalLimits refer to the DocID range returned by the raw segment
	internalLimits segmentLimits
	// externalLimits refer to the transposed DocID exposed to the external
	// users of the compositeSegment
	externalLimits segmentLimits
}

func (s *subSegment) Close() error {
	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(s.reader.Close())
	s.reader = nil
	multiErr = multiErr.Add(s.seg.Close())
	s.seg = nil
	return multiErr.FinalError()
}

func (s *subSegment) Doc(id postings.ID) (doc.Document, error) {
	offset := id - s.externalLimits.startInclusive
	internalID := s.internalLimits.startInclusive + offset
	return s.reader.Doc(internalID)
}

type segmentLimits struct {
	startInclusive postings.ID
	endExclusive   postings.ID
}
