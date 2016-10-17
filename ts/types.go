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

package ts

import (
	"crypto/md5"
	"time"

	"github.com/m3db/m3db/context"
)

// ID represents an immutable identifier for a timeseries
type ID interface {
	context.Closer

	Data() []byte
	Equal(value ID) bool
	Hash() Hash
	String() string
}

// IdentifierPool represents an automatic pool of IDs
type IdentifierPool interface {
	GetBinaryID(context.Context, []byte) ID
	GetStringID(context.Context, string) ID

	// Clone replicates a given ID into a pooled ID
	Clone(other ID) ID
}

// Hash represents a form of ID suitable to be used as map keys
type Hash [md5.Size]byte

// A Datapoint is a single data value reported at a given time
type Datapoint struct {
	Timestamp time.Time
	Value     float64
}

// Annotation represents information used to annotate datapoints
type Annotation []byte
