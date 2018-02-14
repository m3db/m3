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
	"fmt"
	"reflect"
	"testing"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/suite"
)

var (
	testRandomSeed         int64 = 42
	testMinSuccessfulTests       = 1000

	sampleRegexes = []interface{}{
		`a`,
		`a.`,
		`a.b`,
		`ab`,
		`a.b.c`,
		`abc`,
		`a|^`,
		`a|b`,
		`(a)`,
		`(a)|b`,
		`a*`,
		`a+`,
		`a?`,
		`a{2}`,
		`a{2,3}`,
		`a{2,}`,
		`a*?`,
		`a+?`,
		`a??`,
		`a{2}?`,
		`a{2,3}?`,
		`a{2,}?`,
	}
)

type newSimpleTermsDictFn func() *simpleTermsDictionary

type simpleTermsDictionaryTestSuite struct {
	suite.Suite

	fn        newSimpleTermsDictFn
	termsDict *simpleTermsDictionary
}

func (t *simpleTermsDictionaryTestSuite) SetupTest() {
	t.termsDict = t.fn()
}

func (t *simpleTermsDictionaryTestSuite) TestInsert() {
	props := getProperties()
	props.Property(
		"Inserted fields should be immediately searchable",
		prop.ForAll(
			func(f doc.Field, id segment.DocID) (bool, error) {
				err := t.termsDict.Insert(f, id)
				if err != nil {
					return false, fmt.Errorf("unexpected error inserting %v into terms dictionary: %v", f, err)
				}

				ids, err := t.termsDict.Fetch(f.Name, []byte(f.Value), termFetchOptions{false})
				if err != nil {
					return false, fmt.Errorf("unexpected error searching for %v in terms dictionary: %v", f, err)
				}

				if ids == nil {
					return false, fmt.Errorf("ids of documents matching query should not be nil")
				}
				if !ids.Contains(id) {
					return false, fmt.Errorf("id of new document '%v' is not in list of matching documents", id)
				}

				return true, nil
			},
			genField(),
			genDocID(),
		))

	props.TestingRun(t.T())
}

func (t *simpleTermsDictionaryTestSuite) TestInsertIdempotent() {
	props := getProperties()
	props.Property(
		"Inserting the same field into the terms dictionary should be idempotent",
		prop.ForAll(
			func(f doc.Field, id segment.DocID) (bool, error) {
				err := t.termsDict.Insert(f, id)
				if err != nil {
					return false, fmt.Errorf("unexpected error inserting %v into terms dictionary: %v", f, err)
				}

				// Inserting the same field is a valid operation.
				err = t.termsDict.Insert(f, id)
				if err != nil {
					return false, fmt.Errorf("unexpected error re-inserting %v into terms dictionary: %v", f, err)
				}

				ids, err := t.termsDict.Fetch(f.Name, []byte(f.Value), termFetchOptions{false})
				if err != nil {
					return false, fmt.Errorf("unexpected error searching for %v in terms dictionary: %v", f, err)
				}

				if ids == nil {
					return false, fmt.Errorf("ids of documents matching query should not be nil")
				}
				if !ids.Contains(segment.DocID(id)) {
					return false, fmt.Errorf("id of new document '%v' is not in list of matching documents", id)
				}

				return true, nil
			},
			genField(),
			genDocID(),
		))

	props.TestingRun(t.T())
}

func (t *simpleTermsDictionaryTestSuite) TestFetchRegex() {
	props := getProperties()
	props.Property(
		"The dictionary should support regular expression queries",
		prop.ForAll(
			func(input fieldAndRegex, id segment.DocID) (bool, error) {
				var (
					f  = input.field
					re = input.re
				)
				err := t.termsDict.Insert(f, id)
				if err != nil {
					return false, fmt.Errorf("unexpected error inserting %v into terms dictionary: %v", f, err)
				}

				ids, err := t.termsDict.Fetch(f.Name, []byte(re), termFetchOptions{true})
				if err != nil {
					return false, fmt.Errorf("unexpected error searching for %v in terms dictionary: %v", f, err)
				}

				if ids == nil {
					return false, fmt.Errorf("ids of documents matching query should not be nil")
				}
				if !ids.Contains(id) {
					return false, fmt.Errorf("id of new document '%v' is not in list of matching documents", id)
				}

				return true, nil
			},
			genFieldAndRegex(),
			genDocID(),
		))

	props.TestingRun(t.T())
}

func TestSimpleTermsDictionary(t *testing.T) {
	opts := NewOptions()
	suite.Run(t, &simpleTermsDictionaryTestSuite{
		fn: func() *simpleTermsDictionary {
			return newSimpleTermsDictionary(opts)
		},
	})
}

func getProperties() *gopter.Properties {
	params := gopter.DefaultTestParameters()
	params.Rng.Seed(testRandomSeed)
	params.MinSuccessfulTests = testMinSuccessfulTests
	return gopter.NewProperties(params)
}

func genField() gopter.Gen {
	return gopter.CombineGens(
		gen.AnyString(),
		gen.AnyString(),
	).Map(func(values []interface{}) doc.Field {
		var (
			name  = values[0].(string)
			value = values[1].(string)
		)
		f := doc.Field{
			Name:  []byte(name),
			Value: doc.Value(value),
		}
		return f
	})
}

func genDocID() gopter.Gen {
	return gen.UInt32().
		Map(func(value uint32) segment.DocID {
			return segment.DocID(value)
		})
}

type fieldAndRegex struct {
	field doc.Field
	re    string
}

func genFieldAndRegex() gopter.Gen {
	return gen.OneConstOf(sampleRegexes...).
		FlatMap(func(value interface{}) gopter.Gen {
			re := value.(string)
			return fieldFromRegex(re)
		}, reflect.TypeOf(fieldAndRegex{}))
}

func fieldFromRegex(re string) gopter.Gen {
	return gopter.CombineGens(
		gen.AnyString(),
		gen.RegexMatch(re),
	).Map(func(values []interface{}) fieldAndRegex {
		var (
			name  = values[0].(string)
			value = values[1].(string)
		)
		f := doc.Field{
			Name:  []byte(name),
			Value: doc.Value(value),
		}
		return fieldAndRegex{field: f, re: re}
	})
}
