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
	re "regexp"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
)

// ReadableSegment is an internal interface for reading from a segment.
//
// NB(jeromefroe): Currently mockgen requires that interfaces with embedded interfaces be
// generated with reflection mode, but private interfaces can only be generated with file
// mode so we can't mock this interface if its private. Once mockgen supports mocking
// private interfaces which contain embedded interfaces we can make this interface private.
type ReadableSegment interface {
	Fields() (sgmt.FieldsIterator, error)
	ContainsField(field []byte) (bool, error)
	Terms(field []byte) (sgmt.TermsIterator, error)
	TermsWithRegex(field []byte, re *index.CompiledRegex) (sgmt.TermsIterator, error)
	FieldsPostingsList() (sgmt.FieldsPostingsListIterator, error)
	FieldsPostingsListWithRegex(re *index.CompiledRegex) (sgmt.FieldsPostingsListIterator, error)
	matchTerm(field, term []byte) (postings.List, error)
	matchRegexp(field []byte, compiled *re.Regexp) (postings.List, error)
	getDoc(id postings.ID) (doc.Metadata, error)
}
