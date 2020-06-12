// Copyright (c) 2020 Uber Technologies, Inc.
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

package storage

import (
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	xtime "github.com/m3db/m3/src/x/time"
)

// Validate will validate the write query options.
func (o WriteQueryOptions) Validate() error {
	if err := o.validate(); err != nil {
		// NB(r): Always make sure returns invalid params error
		// here so that 4XX is returned to client on remote write endpoint.
		return xerrors.NewInvalidParamsError(err)
	}
	return nil
}

func (o WriteQueryOptions) validate() error {
	if len(o.Datapoints) == 0 {
		return errWriteQueryNoDatapoints
	}
	if err := o.Unit.Validate(); err != nil {
		return err
	}
	// Note: expensive check last.
	if err := o.Tags.Validate(); err != nil {
		return err
	}
	return nil
}

// NewWriteQuery returns a new write query after validating the options.
func NewWriteQuery(opts WriteQueryOptions) (*WriteQuery, error) {
	q := &WriteQuery{}
	if err := q.Reset(opts); err != nil {
		return nil, err
	}
	return q, nil
}

// Reset resets the write query for reuse.
func (q *WriteQuery) Reset(opts WriteQueryOptions) error {
	if err := opts.Validate(); err != nil {
		return err
	}
	q.opts = opts
	return nil
}

// Tags returns the tags.
func (q WriteQuery) Tags() models.Tags {
	return q.opts.Tags
}

// Datapoints returns the datapoints.
func (q WriteQuery) Datapoints() ts.Datapoints {
	return q.opts.Datapoints
}

// Unit returns the unit.
func (q WriteQuery) Unit() xtime.Unit {
	return q.opts.Unit
}

// Annotation returns the annotation.
func (q WriteQuery) Annotation() []byte {
	return q.opts.Annotation
}

// Attributes returns the attributes.
func (q WriteQuery) Attributes() storagemetadata.Attributes {
	return q.opts.Attributes
}

// Validate validates the write query.
func (q *WriteQuery) Validate() error {
	return q.opts.Validate()
}

// Options returns the options used to create the write query.
func (q WriteQuery) Options() WriteQueryOptions {
	return q.opts
}

func (q *WriteQuery) String() string {
	return string(q.opts.Tags.ID())
}
