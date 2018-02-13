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

package mem

import (
	"errors"
	"fmt"
	"regexp/syntax"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"

	"github.com/m3db/codesearch/index"
)

var (
	errUnsupportedQuery = errors.New("query is not supported by the trigram terms dictionary")
)

// trigramsTermsDict stores trigrams of terms to support regular expression queries
// more efficiently. It does not support all queries and returns an errUnsupportedQuery
// for queries which it does not support. It can also return false positives and it is the
// caller's responsibility to check the docIDs returned from Fetch if false positives
// are not acceptable.
//
// The trigram terms dictionary works by breaking down the value of a field into
// its constiuent trigrams and storing each trigram in a simple dictionary. For
// example, given a field (name: "foo", value: "fizzbuzz") and DocID `i` we
//   (1) Compute all trigrams for the given value. In this case:
//         fiz, izz, zzb, zbu, buz, uzz
//   (2) For each trigram `t` created in step 1, store the entry
//       (value: `t`, docID: `i`) in the postings list for the field name "foo".
//
type trigramTermsDictionary struct {
	opts Options

	backingDict *simpleTermsDictionary
}

func newTrigramTermsDictionary(opts Options) *trigramTermsDictionary {
	return &trigramTermsDictionary{
		opts:        opts,
		backingDict: newSimpleTermsDictionary(opts),
	}
}

func (t *trigramTermsDictionary) Insert(field doc.Field, i segment.DocID) error {
	// TODO: Benchmark performance difference between first constructing a set of unique
	// trigrams versus inserting all trigrams and relying on the backing dictionary to
	// deduplicate them.
	trigrams := computeTrigrams(field.Value)
	for _, tri := range trigrams {
		if err := t.backingDict.Insert(
			doc.Field{
				Name:      field.Name,
				Value:     tri,
				ValueType: field.ValueType,
			},
			i,
		); err != nil {
			return fmt.Errorf("unable to insert trigram %s into backing dictionary: %v", tri, err)
		}
	}
	return nil
}

func (t *trigramTermsDictionary) Fetch(
	fieldName []byte,
	fieldValueFilter []byte,
	opts termFetchOptions,
) (segment.PostingsList, error) {
	filter := string(fieldValueFilter)
	re, err := syntax.Parse(filter, syntax.Perl)
	if err != nil {
		return nil, fmt.Errorf("unable to parse query filter %s: %v", filter, err)
	}
	q := index.RegexpQuery(re)
	ids, err := t.postingQuery(fieldName, q, nil, false)
	if err != nil {
		return nil, fmt.Errorf("unable to get postings list matching query: %v", err)
	}
	return ids, nil
}

func (t *trigramTermsDictionary) postingQuery(
	fieldName []byte,
	q *index.Query,
	candidateIDs segment.PostingsList,
	created bool,
) (segment.PostingsList, error) {
	switch q.Op {
	case index.QNone:
		// Do nothing.

	case index.QAll:
		if candidateIDs != nil {
			return candidateIDs, nil
		}
		// Match all queries are not supported by the trigram terms dictionary.
		return nil, errUnsupportedQuery

	case index.QAnd:
		for _, tri := range q.Trigram {
			ids, err := t.docIDsForTrigram(fieldName, tri)
			if err != nil {
				return nil, err
			}
			if ids == nil {
				return t.opts.PostingsListPool().Get(), nil
			}
			if !created {
				candidateIDs = ids.Clone()
				created = true
			} else {
				candidateIDs.Intersect(ids)
			}
			if candidateIDs.IsEmpty() {
				return candidateIDs, nil
			}
		}

		for _, sub := range q.Sub {
			ids, err := t.postingQuery(fieldName, sub, candidateIDs, created)
			if err != nil {
				return nil, err
			}
			if ids == nil {
				return t.opts.PostingsListPool().Get(), nil
			}
			if !created {
				candidateIDs = ids
				created = true
			} else {
				candidateIDs.Intersect(ids)
			}
			if candidateIDs.IsEmpty() {
				return candidateIDs, nil
			}
		}

	case index.QOr:
		for _, tri := range q.Trigram {
			ids, err := t.docIDsForTrigram(fieldName, tri)
			if err != nil {
				return nil, err
			}
			if ids == nil {
				continue
			}
			if !created {
				candidateIDs = ids.Clone()
				created = true
			} else {
				candidateIDs.Union(ids)
			}
		}

		for _, sub := range q.Sub {
			ids, err := t.postingQuery(fieldName, sub, candidateIDs, created)
			if err != nil {
				return nil, err
			}
			if ids == nil {
				return t.opts.PostingsListPool().Get(), nil
			}
			if !created {
				candidateIDs = ids
				created = true
			} else {
				candidateIDs.Union(ids)
			}
		}
	}

	return candidateIDs, nil
}

func (t *trigramTermsDictionary) docIDsForTrigram(
	fieldName []byte,
	tri string,
) (segment.ImmutablePostingsList, error) {
	// TODO(jeromefroe): Consider adding a FetchString method to the simpleDictionary
	// to avoid the string conversion here.
	return t.backingDict.Fetch(fieldName, []byte(tri), termFetchOptions{isRegexp: false})
}

// computeTrigrams returns the trigrams composing a byte slice. The slice of trigrams
// returned is not guaranteed to be unique.
func computeTrigrams(value []byte) [][]byte {
	numTrigrams := len(value) - 2
	if numTrigrams < 1 {
		return nil
	}

	trigrams := make([][]byte, 0, numTrigrams)
	for i := 2; i < len(value); i++ {
		trigrams = append(trigrams, value[i-2:i+1])
	}
	return trigrams
}
