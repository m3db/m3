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

package r2

// UpdateOptions is a set of ruleset or namespace update options.
type UpdateOptions interface {
	// SetAuthor sets the author for an update.
	SetAuthor(value string) UpdateOptions

	// Author returns the author for an update.
	Author() string
}

type updateOptions struct {
	author string
}

// NewUpdateOptions creates a new set of update options.
func NewUpdateOptions() UpdateOptions {
	return &updateOptions{}
}

func (o *updateOptions) SetAuthor(value string) UpdateOptions {
	opts := *o
	opts.author = value
	return &opts
}

func (o *updateOptions) Author() string {
	return o.author
}
