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

package cache

import (
	"context"
	"time"
)

// A LoaderWithTTLFunc is a function for loading entries from a cache, also
// returning an expiration time.
type LoaderWithTTLFunc func(ctx context.Context, key string) (interface{}, time.Time, error)

// A LoaderFunc is a function for loading entries from a cache.
type LoaderFunc func(ctx context.Context, key string) (interface{}, error)

// Cache is an interface for caches.
type Cache interface {
	// Put puts a new item in the cache with the default TTL.
	Put(key string, value interface{})

	// PutWithTTL puts a new item in the cache with a specific TTL.
	PutWithTTL(key string, value interface{}, ttl time.Duration)

	// Get returns the value associated with the key, optionally
	// loading it if it does not exist or has expired.
	// NB(mmihic): We pass the loader as an argument rather than
	// making it a property of the cache to support access specific
	// loading arguments which might not be bundled into the key.
	Get(ctx context.Context, key string, loader LoaderFunc) (interface{}, error)

	// GetWithTTL returns the value associated with the key, optionally
	// loading it if it does not exist or has expired, and allowing the
	// loader to return a TTL for the resulting value, overriding the
	// default TTL associated with the cache.
	GetWithTTL(ctx context.Context, key string, loader LoaderWithTTLFunc) (interface{}, error)
}
