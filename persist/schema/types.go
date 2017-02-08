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

package schema

// IndexInfo stores metadata information about block filesets
type IndexInfo struct {
	Start     int64
	BlockSize int64
	Entries   int64
}

// IndexEntry stores entry-level data for easier indexing
type IndexEntry struct {
	Index    int64
	ID       []byte
	Size     int64
	Offset   int64
	Checksum int64
}

// LogInfo stores summary information about a commit log
type LogInfo struct {
	Start    int64
	Duration int64
	Index    int64
}

// LogEntry stores per-entry data in a commit log
type LogEntry struct {
	Create     int64
	Index      uint64
	Metadata   []byte
	Timestamp  int64
	Value      float64
	Unit       uint32
	Annotation []byte
}

// LogMetadata stores metadata information about a commit log
type LogMetadata struct {
	ID        []byte
	Namespace []byte
	Shard     uint32
}
