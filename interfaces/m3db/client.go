// Copyright (c) 2016 Uber Technologies, Inc.
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

package m3db

import (
	"time"

	"github.com/m3db/m3x/time"
)

// Client can create sessions to write and read to a cluster
type Client interface {
	// NewSession creates a new session
	NewSession() (Session, error)
}

// Session can write and read to a cluster
type Session interface {
	// Write value to the database for an ID
	Write(id string, t time.Time, value float64, unit xtime.Unit, annotation []byte) error

	// Fetch values from the database for an ID
	Fetch(id string, startInclusive, endExclusive time.Time) (SeriesIterator, error)

	// FetchAll values from the database for a set of IDs
	FetchAll(ids []string, startInclusive, endExclusive time.Time) (SeriesIterators, error)

	// Close the session
	Close() error
}
