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
	re "regexp"
	"testing"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/postings"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/suite"
)

var (
	testRandomSeed         int64 = 42
	testMinSuccessfulTests       = 1000

	sampleRegexps = []interface{}{
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

type newTermsDictFn func() *termsDict

type termsDictionaryTestSuite struct {
	suite.Suite

	fn        newTermsDictFn
	termsDict *termsDict
}

func (t *termsDictionaryTestSuite) SetupTest() {
	t.termsDict = t.fn()
}

func (t *termsDictionaryTestSuite) TestInsert() {
	props := getProperties()
	props.Property(
		"The dictionary should support inserting fields",
		prop.ForAll(
			func(f doc.Field, id postings.ID) (bool, error) {
				t.termsDict.Insert(f, id)
				return true, nil
			},
			genField(),
			genDocID(),
		))

	props.TestingRun(t.T())
}

func (t *termsDictionaryTestSuite) TestIterateFields() {
	props := getProperties()
	props.Property(
		"The dictionary should support iterating over known fields",
		prop.ForAll(
			func(genFields []doc.Field, id postings.ID) (bool, error) {
				expectedFields := make(map[string]struct{}, len(genFields))
				for _, f := range genFields {
					t.termsDict.Insert(f, id)
					expectedFields[string(f.Name)] = struct{}{}
				}
				fields := t.termsDict.Fields()
				for _, field := range fields {
					delete(expectedFields, string(field))
				}
				return len(expectedFields) == 0, nil
			},
			gen.SliceOf(genField()),
			genDocID(),
		))
	props.TestingRun(t.T())
}

func (t *termsDictionaryTestSuite) TestIterateTerms() {
	props := getProperties()
	props.Property(
		"The dictionary should support iterating over known terms",
		prop.ForAll(
			func(genFields []doc.Field, id postings.ID) bool {
				// build map from fieldName -> fieldValue of all generated inputs, and insert into terms dict
				expectedFields := make(map[string]map[string]struct{}, len(genFields))
				for _, f := range genFields {
					t.termsDict.Insert(f, id)
					fName, fValue := string(f.Name), string(f.Value)
					vals, ok := expectedFields[fName]
					if !ok {
						vals = make(map[string]struct{})
						expectedFields[fName] = vals
					}
					vals[fValue] = struct{}{}
				}
				// for each expected combination of fieldName -> []fieldValues, ensure all are present
				for name, expectedValues := range expectedFields {
					values := t.termsDict.Terms([]byte(name))
					for _, val := range values {
						delete(expectedValues, string(val))
					}
					if len(expectedValues) != 0 {
						return false
					}
				}
				return true
			},
			gen.SliceOf(genField()),
			genDocID(),
		))
	props.TestingRun(t.T())
}

func (t *termsDictionaryTestSuite) TestContainsTerm() {
	props := getProperties()
	props.Property(
		"The dictionary should support term lookups",
		prop.ForAll(
			func(f doc.Field, id postings.ID) (bool, error) {
				t.termsDict.Insert(f, id)

				if ok := t.termsDict.ContainsTerm(f.Name, []byte(f.Value)); !ok {
					return false, fmt.Errorf("id of new document '%v' is not in postings list of matching documents", id)
				}

				return true, nil
			},
			genField(),
			genDocID(),
		))

	props.TestingRun(t.T())
}

func (t *termsDictionaryTestSuite) TestMatchTerm() {
	props := getProperties()
	props.Property(
		"The dictionary should support exact match queries",
		prop.ForAll(
			func(f doc.Field, id postings.ID) (bool, error) {
				t.termsDict.Insert(f, id)

				pl := t.termsDict.MatchTerm(f.Name, []byte(f.Value))
				if pl == nil {
					return false, fmt.Errorf("postings list of documents matching query should not be nil")
				}
				if !pl.Contains(id) {
					return false, fmt.Errorf("id of new document '%v' is not in postings list of matching documents", id)
				}

				return true, nil
			},
			genField(),
			genDocID(),
		))

	props.TestingRun(t.T())
}

func (t *termsDictionaryTestSuite) TestMatchTermNoResults() {
	props := getProperties()
	props.Property(
		"Exact match queries which return no results are valid",
		prop.ForAll(
			func(f doc.Field) (bool, error) {
				pl := t.termsDict.MatchTerm(f.Name, []byte(f.Value))
				if pl == nil {
					return false, fmt.Errorf("postings list returned should not be nil")
				}
				if pl.Len() != 0 {
					return false, fmt.Errorf("postings list contains unexpected IDs")
				}

				return true, nil
			},
			genField(),
		))

	props.TestingRun(t.T())
}

func (t *termsDictionaryTestSuite) TestMatchRegex() {
	props := getProperties()
	props.Property(
		"The dictionary should support regular expression queries",
		prop.ForAll(
			func(input fieldAndRegexp, id postings.ID) (bool, error) {
				var (
					f        = input.field
					regexp   = input.regexp
					compiled = input.compiled
				)

				t.termsDict.Insert(f, id)

				pl := t.termsDict.MatchRegexp(f.Name, []byte(regexp), compiled)
				if pl == nil {
					return false, fmt.Errorf("postings list of documents matching query should not be nil")
				}
				if !pl.Contains(id) {
					return false, fmt.Errorf("id of new document '%v' is not in list of matching documents", id)
				}

				return true, nil
			},
			genFieldAndRegex(),
			genDocID(),
		))

	props.TestingRun(t.T())
}

func (t *termsDictionaryTestSuite) TestMatchRegexNoResults() {
	props := getProperties()
	props.Property(
		"Regular expression queries which no results are valid",
		prop.ForAll(
			func(input fieldAndRegexp, id postings.ID) (bool, error) {
				var (
					f        = input.field
					regexp   = input.regexp
					compiled = input.compiled
				)
				pl := t.termsDict.MatchRegexp(f.Name, []byte(regexp), compiled)
				if pl == nil {
					return false, fmt.Errorf("postings list returned should not be nil")
				}
				if pl.Len() != 0 {
					return false, fmt.Errorf("postings list contains unexpected IDs")
				}

				return true, nil
			},
			genFieldAndRegex(),
			genDocID(),
		))

	props.TestingRun(t.T())
}

func TestTermsDictionary(t *testing.T) {
	opts := NewOptions()
	suite.Run(t, &termsDictionaryTestSuite{
		fn: func() *termsDict {
			return newTermsDict(opts).(*termsDict)
		},
	})
}

func getProperties() *gopter.Properties {
	params := gopter.DefaultTestParameters()
	params.MaxSize = 10
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
			Value: []byte(value),
		}
		return f
	})
}

func genDocID() gopter.Gen {
	return gen.UInt32().
		Map(func(value uint32) postings.ID {
			return postings.ID(value)
		})
}

type fieldAndRegexp struct {
	field    doc.Field
	regexp   string
	compiled *re.Regexp
}

func genFieldAndRegex() gopter.Gen {
	return gen.OneConstOf(sampleRegexps...).
		FlatMap(func(value interface{}) gopter.Gen {
			regex := value.(string)
			return fieldFromRegexp(regex)
		}, reflect.TypeOf(fieldAndRegexp{}))
}

func fieldFromRegexp(regexp string) gopter.Gen {
	return gopter.CombineGens(
		gen.AnyString(),
		gen.RegexMatch(regexp),
	).Map(func(values []interface{}) fieldAndRegexp {
		var (
			name  = values[0].(string)
			value = values[1].(string)
		)
		f := doc.Field{
			Name:  []byte(name),
			Value: []byte(value),
		}
		return fieldAndRegexp{
			field:    f,
			regexp:   regexp,
			compiled: re.MustCompile(regexp),
		}
	})
}
