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

// GenAllQuery generates an all query.
func GenAllQuery(docs []doc.Metadata) gopter.Gen {
	return gen.Const(query.NewAllQuery())
}

// GenFieldQuery generates a field query.
func GenFieldQuery(docs []doc.Metadata) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		fieldName, _ := fieldNameAndValue(genParams, docs)
		q := query.NewFieldQuery(fieldName)
		return gopter.NewGenResult(q, gopter.NoShrinker)
	}
}

// GenTermQuery generates a term query.
func GenTermQuery(docs []doc.Metadata) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		fieldName, fieldValue := fieldNameAndValue(genParams, docs)
		q := query.NewTermQuery(fieldName, fieldValue)
		return gopter.NewGenResult(q, gopter.NoShrinker)
	}
}

func fieldNameAndValue(genParams *gopter.GenParameters, docs []doc.Metadata) ([]byte, []byte) {
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
	return field.Name, field.Value
}

// GenIdenticalTermAndRegexpQuery generates a term query and regexp query with
// the exact same underlying field and pattern.
func GenIdenticalTermAndRegexpQuery(docs []doc.Metadata) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		fieldName, fieldValue := fieldNameAndValue(genParams, docs)
		termQ := query.NewTermQuery(fieldName, fieldValue)
		regexpQ, err := query.NewRegexpQuery(fieldName, fieldValue)
		if err != nil {
			panic(err)
		}
		return gopter.NewGenResult([]search.Query{termQ, regexpQ}, gopter.NoShrinker)
	}
}

// GenRegexpQuery generates a regexp query.
func GenRegexpQuery(docs []doc.Metadata) gopter.Gen {
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

// GenNegationQuery generates a negation query.
func GenNegationQuery(docs []doc.Metadata) gopter.Gen {
	return gen.OneGenOf(
		GenFieldQuery(docs),
		GenTermQuery(docs),
		GenRegexpQuery(docs),
	).
		Map(func(q search.Query) search.Query {
			return query.NewNegationQuery(q)
		})
}

// GenConjunctionQuery generates a conjunction query.
func GenConjunctionQuery(docs []doc.Metadata) gopter.Gen {
	return gen.SliceOf(
		gen.OneGenOf(
			GenFieldQuery(docs),
			GenTermQuery(docs),
			GenRegexpQuery(docs),
			GenNegationQuery(docs)),
		reflect.TypeOf((*search.Query)(nil)).Elem()).
		Map(func(qs []search.Query) search.Query {
			return query.NewConjunctionQuery(qs)
		})
}

// GenDisjunctionQuery generates a disjunction query.
func GenDisjunctionQuery(docs []doc.Metadata) gopter.Gen {
	return gen.SliceOf(
		gen.OneGenOf(
			GenFieldQuery(docs),
			GenTermQuery(docs),
			GenRegexpQuery(docs),
			GenNegationQuery(docs)),
		reflect.TypeOf((*search.Query)(nil)).Elem()).
		Map(func(qs []search.Query) search.Query {
			return query.NewDisjunctionQuery(qs)
		})
}

// GenQuery generates a query.
func GenQuery(docs []doc.Metadata) gopter.Gen {
	return gen.OneGenOf(
		GenAllQuery(docs),
		GenFieldQuery(docs),
		GenTermQuery(docs),
		GenRegexpQuery(docs),
		GenNegationQuery(docs),
		GenConjunctionQuery(docs),
		GenDisjunctionQuery(docs))
}
