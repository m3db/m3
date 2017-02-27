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

package kv

import (
	"errors"

	"github.com/golang/protobuf/proto"
)

var (
	// ErrVersionMismatch is returned when attempting a CheckAndSet and the
	// key is not at the provided version
	ErrVersionMismatch = errors.New("key is not at the specified version")

	// ErrAlreadyExists is returned when attempting a SetIfEmpty and the key
	// already has a value
	ErrAlreadyExists = errors.New("key already has a value")

	// ErrNotFound is returned when attempting a Get but no value is found for
	// the given key
	ErrNotFound = errors.New("key not found")
)

// A Value provides access to a versioned value in the configuration store
type Value interface {
	// Unmarshal retrieves the stored value
	Unmarshal(v proto.Message) error

	// Version returns the current version of the value
	Version() int

	// IsNewer returns if this Value is newer than the other Value
	IsNewer(other Value) bool
}

// ValueWatch provides updates to a Value
type ValueWatch interface {
	// C returns the notification channel
	C() <-chan struct{}
	// Get returns the latest version of the value
	Get() Value
	// Close stops watching for value updates
	Close()
}

// ValueWatchable can be watched for Value changes
type ValueWatchable interface {
	// Get returns the latest Value
	Get() Value
	// Watch returns the Value and a ValueWatch that will be notified on updates
	Watch() (Value, ValueWatch, error)
	// NumWatches returns the number of watches on the Watchable
	NumWatches() int
	// Update sets the Value and notify Watches
	Update(Value) error
	// IsClosed returns true if the Watchable is closed
	IsClosed() bool
	// Close stops watching for value updates
	Close()
}

// Store provides access to the configuration store
type Store interface {
	// Get retrieves the value for the given key
	Get(key string) (Value, error)

	// Watch adds a watch for value updates for given key. This is a non-blocking
	// call - a notification will be sent to ValueWatch.C() once a value is
	// available
	Watch(key string) (ValueWatch, error)

	// Set stores the value for the given key
	Set(key string, v proto.Message) (int, error)

	// SetIfNotExists sets the value for the given key only if no value already
	// exists
	SetIfNotExists(key string, v proto.Message) (int, error)

	// CheckAndSet stores the value for the given key if the current version
	// matches the provided version
	CheckAndSet(key string, version int, v proto.Message) (int, error)

	// Delete deletes a key in the store and returns the last value before deletion
	Delete(key string) (Value, error)

	// History returns the value for a key in version range [from, to)
	History(key string, from, to int) ([]Value, error)
}
