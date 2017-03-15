// Copyright (c) 2016 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package encoding

import "github.com/m3db/m3db/persist/schema"

// Encoder encodes data for persistence
type Encoder interface {
	// Reset resets the buffer
	Reset()

	// Bytes returns the encoded bytes
	Bytes() []byte

	// EncodeIndexInfo encodes index info
	EncodeIndexInfo(info schema.IndexInfo) error

	// EncodeIndexEntry encodes index entry
	EncodeIndexEntry(entry schema.IndexEntry) error

	// EncodeLogInfo encodes commit log info
	EncodeLogInfo(info schema.LogInfo) error

	// EncodeLogEntry encodes commit log entry
	EncodeLogEntry(entry schema.LogEntry) error

	// EncodeLogMetadata encodes commit log metadata
	EncodeLogMetadata(metadata schema.LogMetadata) error
}
