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
	"fmt"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3x/checked"
)

// ID represents an immutable identifier for a timeseries.
type ID interface {
	fmt.Stringer

	Data() checked.Bytes
	Hash() Hash
	Equal(value ID) bool

	Finalize()
	Reset()
}

// TagName represents the name of a timeseries tag.
type TagName ID

// TagValue represents the value of a timeseries tag.
type TagValue ID

// Tag represents a timeseries tag.
type Tag struct {
	Name  TagName
	Value TagValue
}

// IdentifierPool represents an automatic pool of IDs.
type IdentifierPool interface {
	// GetBinaryID will create a new binary ID and take reference to the bytes.
	// When the context closes the ID will be finalized and so too will
	// the bytes, i.e. it will take ownership of the bytes.
	GetBinaryID(context.Context, checked.Bytes) ID

	// GetBinaryTag will create a new binary Tag and take reference to the bytes.
	// When the context closes, the Tag will be finalized and so too will
	// the bytes, i.e. it will take ownership of the bytes.
	GetBinaryTag(c context.Context, name checked.Bytes, value checked.Bytes) Tag

	// GetStringID will create a new string ID and create a bytes copy of the
	// string. When the context closes the ID will be finalized.
	GetStringID(context.Context, string) ID

	// GetStringTag will create a new string Tag and create a bytes copy of the
	// string. When the context closes the ID will be finalized.
	GetStringTag(c context.Context, name string, value string) Tag

	// Put an ID back in the pool.
	Put(ID)

	// PutTag puts a tag back in the pool.
	PutTag(Tag)

	// Clone replicates a given ID into a pooled ID.
	Clone(other ID) ID
}

// Hash represents a form of ID suitable to be used as map keys.
type Hash [md5.Size]byte

// A Datapoint is a single data value reported at a given time.
type Datapoint struct {
	Timestamp time.Time
	Value     float64
}

// Annotation represents information used to annotate datapoints.
type Annotation []byte
