// Copyright (c) 2019 Uber Technologies, Inc.
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

package handleroptions

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
)

func matchByName(name string) (models.MatchType, error) {
	t := models.MatchEqual
	switch name {
	case "EQUAL":
		// noop
	case "NOTEQUAL":
		t = models.MatchNotEqual
	case "REGEXP":
		t = models.MatchRegexp
	case "NOTREGEXP":
		t = models.MatchNotRegexp
	case "EXISTS":
		t = models.MatchField
	case "NOTEXISTS":
		t = models.MatchNotField
	case "ALL":
		return t, errors.New("ALL type not supported as a tag matcher restriction")
	default:
		return t, fmt.Errorf("matcher type %s not recognized", name)
	}

	return t, nil
}

func (m StringMatch) toMatcher() (models.Matcher, error) {
	t, err := matchByName(m.Type)
	if err != nil {
		return models.Matcher{}, err
	}

	return models.NewMatcher(t, []byte(m.Name), []byte(m.Value))
}

// Validate validates the string tag options.
func (o *StringTagOptions) Validate() error {
	_, err := o.StorageOptions()
	return err
}

// StorageOptions returns the corresponding storage.RestrictByTag options.
func (o *StringTagOptions) StorageOptions() (*storage.RestrictByTag, error) {
	if len(o.Restrict) == 0 && len(o.Strip) == 0 {
		return nil, nil
	}

	opts := &storage.RestrictByTag{}
	if len(o.Restrict) > 0 {
		opts.Restrict = make(models.Matchers, 0, len(o.Restrict))
		for _, m := range o.Restrict {
			r, err := m.toMatcher()
			if err != nil {
				return nil, err
			}

			opts.Restrict = append(opts.Restrict, r)
		}
	}

	if o.Strip != nil {
		opts.Strip = make([][]byte, 0, len(o.Strip))
		for _, s := range o.Strip {
			opts.Strip = append(opts.Strip, []byte(s))
		}
	} else {
		// If strip not explicitly set, strip tag names from
		// the restricted matchers.
		opts.Strip = make([][]byte, 0, len(opts.Restrict))
		for _, s := range opts.Restrict {
			opts.Strip = append(opts.Strip, s.Name)
		}
	}

	return opts, nil
}

// StringMatch is an easy to use JSON representation of models.Matcher that
// allows plaintext fields rather than forcing base64 encoded values.
type StringMatch struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// StringTagOptions is an easy to use JSON representation of
// storage.RestrictByTag that allows plaintext string fields rather than
// forcing base64 encoded values.
type StringTagOptions struct {
	Restrict []StringMatch `json:"match"`
	Strip    []string      `json:"strip"`
}

// MapTagsOptions representations mutations to be applied to all timeseries in a
// write request.
type MapTagsOptions struct {
	TagMappers []TagMapper `json:"tagMappers"`
}

// TagMapper represents one of a variety of tag mapping operations.
type TagMapper struct {
	Write         WriteOp         `json:"write,omitEmpty"`
	Drop          DropOp          `json:"drop,omitEmpty"`
	DropWithValue DropWithValueOp `json:"dropWithValue,omitEmpty"`
	Replace       ReplaceOp       `json:"replace,omitEmpty"`
}

// Validate ensures the mapper is valid.
func (t TagMapper) Validate() error {
	numOps := 0
	if !t.Write.IsEmpty() {
		numOps++
	}

	if !t.Drop.IsEmpty() {
		numOps++
	}

	if !t.DropWithValue.IsEmpty() {
		numOps++
	}

	if !t.Replace.IsEmpty() {
		numOps++
	}

	if numOps == 1 {
		return nil
	}

	return fmt.Errorf("must specify one operation per tag mapper (got %d)", numOps)
}

// WriteOp with value tag="foo" and value="bar" will unconditionally add
// tag-value pair "foo":"bar" to all timeseries included in the write request.
// Any timeseries with a non-empty "foo" tag will have its value for that tag
// replaced.
type WriteOp struct {
	Tag   string `json:"tag"`
	Value string `json:"value"`
}

// IsEmpty returns true if the operation is empty.
func (op WriteOp) IsEmpty() bool {
	return op.Tag == "" && op.Value == ""
}

// DropOp with tag="foo" and an empty value will remove all tag-value pairs in
// all timeseries in the write request where the tag was "foo".
type DropOp struct {
	Tag string `json:"tag"`
}

// IsEmpty returns true if the operation is empty.
func (op DropOp) IsEmpty() bool {
	return op.Tag == ""
}

// DropWithValueOp will remove all tag-value pairs in all timeseries in the
// writer equest if and only if the tag AND value in the timeseries is equal to
// those on the operation.
type DropWithValueOp struct {
	Tag   string `json:"tag"`
	Value string `json:"value"`
}

// IsEmpty returns true if the operation is empty.
func (op DropWithValueOp) IsEmpty() bool {
	return op.Tag == "" && op.Value == ""
}

// ReplaceOp with tag="foo", an empty old field, and a non-empty new field will
// unconditionally replace the value of any tag-value pair of any timeseries in
// the write request where the tag is "foo" with the value of new. If old is
// non-empty, a value will only be replaced if the value was equal to old.
type ReplaceOp struct {
	Tag      string `json:"tag"`
	OldValue string `json:"old"`
	NewValue string `json:"new"`
}

// IsEmpty returns true if the operation is empty.
func (op ReplaceOp) IsEmpty() bool {
	return op.Tag == "" && op.OldValue == "" && op.NewValue == ""
}
