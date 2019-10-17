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

package debug

import (
	"fmt"
	"io"
	"net/http"
	"runtime/pprof"
)

// ProfileSource defines a custom source of a runtime/pprof Profile. This can
// either be part of a predefined or custom profile.
type ProfileSource struct {
	name  string
	debug int
}

// NewProfileSource returns a ProfileSource with a name and debug level, where
// the verbosity of the final stack information increases with value.
// Will return an error if name is an empty string.
func NewProfileSource(name string, debug int) (*ProfileSource, error) {
	if name == "" {
		return nil, fmt.Errorf("name can't be empty")
	}
	return &ProfileSource{
		name:  name,
		debug: debug,
	}, nil
}

// Profile returns a *pprof.Profile according the the name of the ProfileSource.
// This will first try find an existing profile, creating one if it can't be
// found.
func (p *ProfileSource) Profile() *pprof.Profile {
	var pp *pprof.Profile
	pp = pprof.Lookup(p.name)
	if pp == nil {
		return pprof.NewProfile(p.name)
	}
	return pp
}

// Write writes a pprof-formatted snapshot of the profile to w. If a write to w
// returns an error, Write returns that error. Otherwise, Write returns nil.
func (p *ProfileSource) Write(w io.Writer, _ *http.Request) error {
	prof := p.Profile()

	if err := prof.WriteTo(w, p.debug); err != nil {
		return fmt.Errorf("unable to write %s profile: %s", p.name, err)
	}
	return nil
}
