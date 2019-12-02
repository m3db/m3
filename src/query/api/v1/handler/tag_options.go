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

package handler

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

func (m stringMatch) toMatcher() (models.Matcher, error) {
	t, err := matchByName(m.Type)
	if err != nil {
		return models.Matcher{}, err
	}

	return models.NewMatcher(t, []byte(m.Name), []byte(m.Value))
}

func (o *stringTagOptions) toOptions() (*storage.RestrictByTag, error) {
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

type stringMatch struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// This is an easy to use JSON representation of storage.RestrictByTag that
// allows plaintext string fields rather than forcing base64 encoded values.
type stringTagOptions struct {
	Restrict []stringMatch `json:"match"`
	Strip    []string      `json:"strip"`
}
