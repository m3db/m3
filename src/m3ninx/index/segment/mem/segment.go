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

package mem

import (
	"errors"
	"fmt"
	re "regexp"
	"sync"
	"unicode/utf8"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index"
	sgmt "github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/postings"
)

var (
	errSegmentSealed     = errors.New("segment has been sealed")
	errUnknownPostingsID = errors.New("unknown postings ID specified")
)

type segment struct {
	opts   Options
	offset int

	// Internal state of the segment. The allowed transitions are:
	//   - Open -> Sealed -> Closed
	//   - Open -> Closed
	state struct {
		sync.RWMutex

		sealed bool
		closed bool
	}

	// Mapping of postings list ID to document.
	docs struct {
		sync.RWMutex

		data []doc.Document
	}

	// Current writer and reader IDs. Writers increment the writer ID for each new
	// document and only increment the reader ID after the document has been fully
	// indexed by the segment. Readers do not need to acquire a lock since the writers
	// only increment the readerID after they have finished inserting a new document.
	// TODO(jeromefroe): Reevaluate using a big lock here which only permits a single
	// writer at a time.
	ids struct {
		sync.RWMutex

		writer, reader postings.AtomicID
	}

	// Mapping of field (name and value) to postings list.
	termsDict termsDict
}

// NewSegment returns a new in-memory mutable segment. It will start assigning
// postings IDs at offset+1.
func NewSegment(offset postings.ID, opts Options) (sgmt.MutableSegment, error) {
	s := &segment{
		opts:      opts,
		offset:    int(offset) + 1, // The first ID assigned by the segment is offset + 1.
		termsDict: newSimpleTermsDict(opts),
	}

	s.docs.data = make([]doc.Document, opts.InitialCapacity())

	s.ids.writer = postings.NewAtomicID(offset)
	s.ids.reader = postings.NewAtomicID(offset)
	return s, nil
}

func (s *segment) Insert(d doc.Document) error {
	s.state.RLock()
	if s.state.closed {
		s.state.RUnlock()
		return sgmt.ErrClosed
	}

	if s.state.sealed {
		s.state.RUnlock()
		return errSegmentSealed
	}

	// Validate that the document contains only valid UTF-8.
	for _, f := range d.Fields {
		if !utf8.Valid(f.Name) {
			return fmt.Errorf("document contains invalid field name: %v", f.Name)
		}
		if !utf8.Valid(f.Value) {
			return fmt.Errorf("document contains invalid field value: %v", f.Value)
		}
	}

	// TODO: Consider supporting concurrent writes by relaxing the requirement that
	// inserted documents are immediately searchable.
	s.ids.Lock()

	newID := s.ids.writer.Inc()
	s.insertDoc(newID, d)
	err := s.insertTerms(newID, d)
	s.ids.reader.Inc()

	s.ids.Unlock()

	s.state.RUnlock()
	return err
}

func (s *segment) Reader() (index.Reader, error) {
	s.state.RLock()
	if s.state.closed {
		s.state.RUnlock()
		return nil, sgmt.ErrClosed
	}

	maxID := s.ids.reader.Load()
	r := newReader(s, maxID)
	return r, nil
}

func (s *segment) Seal() error {
	s.state.Lock()
	if s.state.sealed {
		s.state.Unlock()
		return errSegmentSealed
	}

	s.state.sealed = true
	s.state.Unlock()
	return nil
}

func (s *segment) Close() error {
	s.state.Lock()
	if s.state.closed {
		s.state.Unlock()
		return sgmt.ErrClosed
	}

	s.state.sealed = true
	s.state.closed = true
	s.state.Unlock()
	return nil
}

func (s *segment) insertDoc(id postings.ID, d doc.Document) {
	idx := int(id) - s.offset

	s.docs.RLock()
	size := len(s.docs.data)

	// Can terminate early if we have sufficient capacity.
	// TODO: Consider enforcing a maximum capacity on the segment so that we can use a
	// fixed-size slice here and avoid the lock altogether.
	if size > idx {
		// NB(prateek): We only need a Read-lock here despite an insert operation because
		// we're guaranteed to never have conflicts with docID (it's monotonically increasing),
		// and have checked `i.docs.data` is large enough.
		s.docs.data[idx] = d
		s.docs.RUnlock()
		return
	}
	s.docs.RUnlock()

	// Otherwise we need to expand capacity.
	s.docs.Lock()
	size = len(s.docs.data)

	// The slice has already been expanded since we released the lock.
	if size > idx {
		s.docs.data[idx] = d
		s.docs.Unlock()
		return
	}

	data := make([]doc.Document, 2*(size+1))
	copy(data, s.docs.data)
	s.docs.data = data
	s.docs.data[idx] = d
	s.docs.Unlock()
}

func (s *segment) insertTerms(id postings.ID, d doc.Document) error {
	for _, f := range d.Fields {
		if err := s.termsDict.Insert(f, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *segment) matchTerm(field, term []byte) (postings.List, error) {
	s.state.RLock()
	if s.state.closed {
		s.state.RUnlock()
		return nil, sgmt.ErrClosed
	}

	return s.termsDict.MatchTerm(field, term)
}

func (s *segment) matchRegexp(name, regexp []byte, compiled *re.Regexp) (postings.List, error) {
	s.state.RLock()
	if s.state.closed {
		s.state.RUnlock()
		return nil, sgmt.ErrClosed
	}

	if compiled == nil {
		var err error
		compiled, err = re.Compile(string(regexp))
		if err != nil {
			return nil, err
		}
	}
	return s.termsDict.MatchRegexp(name, regexp, compiled)
}

func (s *segment) getDoc(id postings.ID) (doc.Document, error) {
	s.state.RLock()
	if s.state.closed {
		s.state.RUnlock()
		return doc.Document{}, sgmt.ErrClosed
	}

	idx := int(id) - s.offset

	s.docs.RLock()
	if idx >= len(s.docs.data) {
		s.docs.RUnlock()
		return doc.Document{}, errUnknownPostingsID
	}
	d := s.docs.data[idx]
	s.docs.RUnlock()

	return d, nil
}
