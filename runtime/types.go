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

package runtime

import (
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3x/close"
)

// Options is a set of runtime options
type Options interface {
	// SetPersistRateLimitOptions sets the persist rate limit options
	SetPersistRateLimitOptions(value ratelimit.Options) Options

	// PersistRateLimitOptions returns the persist rate limit options
	PersistRateLimitOptions() ratelimit.Options

	// SetWriteNewSeriesAsync sets whether to write new series asynchronously or not,
	// when true this essentially makes writes for new series eventually consistent
	// as after a write is finished you are not guarenteed to read it back immediately
	// due to inserts into the shard map being buffered. The write is however written
	// to the commit log before completing so it is considered durable.
	SetWriteNewSeriesAsync(value bool) Options

	// WriteNewSeriesAsync returns whether to write new series asynchronously or not,
	// when true this essentially makes writes for new series eventually consistent
	// as after a write is finished you are not guarenteed to read it back immediately
	// due to inserts into the shard map being buffered. The write is however written
	// to the commit log before completing so it is considered durable.
	WriteNewSeriesAsync() bool
}

// OptionsManager updates and supplies runtime options
type OptionsManager interface {
	// Update updates the current runtime options
	Update(value Options)

	// Get returns the current values
	Get() Options

	// RegisterListener registers a listener for updates to runtime options,
	// it will synchronously call back the listener when this method is called
	// to deliver the current set of runtime options
	RegisterListener(l OptionsListener) xclose.SimpleCloser

	// Close closes the watcher and all descendent watches
	Close()
}

// OptionsListener listens for updates to runtime options
type OptionsListener interface {
	// SetRuntimeOptions is called when the listener is registered
	// and when any updates occurred passing the new runtime options
	SetRuntimeOptions(value Options)
}
