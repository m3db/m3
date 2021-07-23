// Copyright (c) 2021 Uber Technologies, Inc.
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

// Package source identifies the source of query requests.
package source

import (
	"context"
)

type key int

const (
	typedKey key = iota
	rawKey
)

// Deserializer deserializes the raw source bytes into a type for easier use.
// The raw source can be nil and the Deserializer can return a typed empty value for the application.
type Deserializer func([]byte) (interface{}, error)

// NewContext returns a new context with the source bytes as a value if the source is non-nil.
// If a non-nil deserializer is provided an additional typed value is added for easier use.
func NewContext(ctx context.Context, source []byte, deserialize Deserializer) (context.Context, error) {
	if source == nil {
		return ctx, nil
	}
	ctx = context.WithValue(ctx, rawKey, source)
	if deserialize != nil {
		typed, err := deserialize(source)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, typedKey, typed)
	}
	return ctx, nil
}

// FromContext extracts the typed source, or false if it doesn't exist.
func FromContext(ctx context.Context) (interface{}, bool) {
	typed := ctx.Value(typedKey)
	if typed == nil {
		return nil, false
	}
	return typed, true
}

// RawFromContext extracts the raw bytes of the source, or false if it doesn't exist.
// This is used by middleware to propagate the source across API boundaries. Application code should use FromContext.
func RawFromContext(ctx context.Context) ([]byte, bool) {
	b, ok := ctx.Value(rawKey).([]byte)
	return b, ok
}
