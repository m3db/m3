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
	re "regexp"
	"sync"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment/mem/fieldsgen"
	"github.com/m3db/m3ninx/index/segment/mem/postingsgen"
	"github.com/m3db/m3ninx/postings"
)

// simpleTermsDict uses a two-level map to model a terms dictionary. It maps a field
// (name and value) to a postings list.
type simpleTermsDict struct {
	opts Options

	fields struct {
		sync.RWMutex
		internalMap *fieldsgen.Map
	}
}

func newSimpleTermsDict(opts Options) termsDict {
	dict := &simpleTermsDict{
		opts: opts,
	}
	dict.fields.internalMap = fieldsgen.New(opts.InitialCapacity())
	return dict
}

func (t *simpleTermsDict) Insert(field doc.Field, id postings.ID) error {
	postingsMap := t.getOrAddName(field.Name)
	return postingsMap.Add(field.Value, id)
}

func (t *simpleTermsDict) MatchTerm(field, term []byte) (postings.List, error) {
	t.fields.RLock()
	postingsMap, ok := t.fields.internalMap.Get(field)
	t.fields.RUnlock()
	if !ok {
		// It is not an error to not have any matching values.
		return t.opts.PostingsListPool().Get(), nil
	}
	pl := postingsMap.Get(term)

	// Return of the clone of the postings list so that its lifetime is independent of
	// that of the terms dictionary.
	return pl.Clone(), nil
}

func (t *simpleTermsDict) MatchRegexp(
	field, regexp []byte,
	compiled *re.Regexp,
) (postings.List, error) {
	t.fields.RLock()
	postingsMap, ok := t.fields.internalMap.Get(field)
	t.fields.RUnlock()
	if !ok {
		// It is not an error to not have any matching values.
		return t.opts.PostingsListPool().Get(), nil
	}

	pls := postingsMap.GetRegex(compiled)
	union := t.opts.PostingsListPool().Get()
	for _, pl := range pls {
		union.Union(pl)
	}
	return union, nil
}

func (t *simpleTermsDict) getOrAddName(name []byte) *postingsgen.ConcurrentMap {
	// Cheap read lock to see if it already exists.
	t.fields.RLock()
	postingsMap, ok := t.fields.internalMap.Get(name)
	t.fields.RUnlock()
	if ok {
		return postingsMap
	}

	// Acquire write lock and create.
	t.fields.Lock()
	postingsMap, ok = t.fields.internalMap.Get(name)

	// Check if it's been created since we last acquired the lock.
	if ok {
		t.fields.Unlock()
		return postingsMap
	}

	postingsMap = postingsgen.NewConcurrentMap(postingsgen.ConcurrentMapOpts{
		InitialSize:      t.opts.InitialCapacity(),
		PostingsListPool: t.opts.PostingsListPool(),
	})
	t.fields.internalMap.SetUnsafe(name, postingsMap, fieldsgen.SetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
	t.fields.Unlock()
	return postingsMap
}
