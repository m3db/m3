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

package proptest

import (
	"bytes"
	"reflect"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/query"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
)

func genTermQuery(docs []doc.Document) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		docIDRes, ok := gen.IntRange(0, len(docs)-1)(genParams).Retrieve()
		if !ok {
			panic("unable to generate term query") // should never happen
		}
		docID := docIDRes.(int)

		doc := docs[docID]
		fieldRes, ok := gen.IntRange(0, len(doc.Fields)-1)(genParams).Retrieve()
		if !ok {
			panic("unable to generate term query fields") // should never happen
		}

		fieldID := fieldRes.(int)
		field := doc.Fields[fieldID]

		q := query.NewTermQuery(field.Name, field.Value)
		return gopter.NewGenResult(q, gopter.NoShrinker)
	}
}

func genRegexpQuery(docs []doc.Document) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		docIDRes, ok := gen.IntRange(0, len(docs)-1)(genParams).Retrieve()
		if !ok {
			panic("unable to generate regexp query") // should never happen
		}
		docID := docIDRes.(int)

		doc := docs[docID]
		fieldRes, ok := gen.IntRange(0, len(doc.Fields)-1)(genParams).Retrieve()
		if !ok {
			panic("unable to generate regexp query fields") // should never happen
		}

		fieldID := fieldRes.(int)
		field := doc.Fields[fieldID]

		var re []byte

		reType := genParams.NextUint64() % 3
		switch reType {
		case 0: // prefix
			idx := genParams.NextUint64() % uint64(len(field.Value))
			re = append([]byte(nil), field.Value[:idx]...)
			re = append(re, []byte(".*")...)
		case 1: // suffix
			idx := genParams.NextUint64() % uint64(len(field.Value))
			re = append([]byte(".*"), field.Value[idx:]...)
		case 2: // middle
			start := genParams.NextUint64() % uint64(len(field.Value))
			remain := uint64(len(field.Value)) - start
			end := start + genParams.NextUint64()%remain
			re = append(append([]byte(".*"), field.Value[start:end]...), []byte(".*")...)
		}

		// escape any '(' or ')' we see to avoid regular expression parsing failure
		escapeFront := bytes.Replace(re, []byte("("), []byte("\\("), -1)
		escapeBack := bytes.Replace(escapeFront, []byte(")"), []byte("\\)"), -1)

		q, err := query.NewRegexpQuery(field.Name, escapeBack)
		if err != nil {
			panic(err)
		}

		return gopter.NewGenResult(q, gopter.NoShrinker)
	}
}

func genNegationQuery(docs []doc.Document) gopter.Gen {
	return gen.OneGenOf(
		genTermQuery(docs),
		genRegexpQuery(docs),
	).
		Map(func(q search.Query) search.Query {
			return query.NewNegationQuery(q)
		})
}

func genConjuctionQuery(docs []doc.Document) gopter.Gen {
	return gen.SliceOf(
		gen.OneGenOf(
			genTermQuery(docs),
			genRegexpQuery(docs),
			genNegationQuery(docs)),
		reflect.TypeOf((*search.Query)(nil)).Elem()).
		Map(func(qs []search.Query) search.Query {
			return query.NewConjunctionQuery(qs)
		})
}

func genDisjunctionQuery(docs []doc.Document) gopter.Gen {
	return gen.SliceOf(
		gen.OneGenOf(
			genTermQuery(docs),
			genRegexpQuery(docs),
			genNegationQuery(docs)),
		reflect.TypeOf((*search.Query)(nil)).Elem()).
		Map(func(qs []search.Query) search.Query {
			return query.NewDisjunctionQuery(qs)
		})
}

func genQuery(docs []doc.Document) gopter.Gen {
	return gen.OneGenOf(
		genTermQuery(docs),
		genRegexpQuery(docs),
		genNegationQuery(docs),
		genConjuctionQuery(docs),
		genDisjunctionQuery(docs))
}
