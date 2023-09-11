// Copyright (c) 2022 Uber Technologies, Inc.
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

package main

import (
	"errors"
	"io"
	"log"
	"strings"

	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
)

type filteringReader struct {
	reader       commitlog.Reader
	idFilter     *string
	idSizeFilter *int
}

func newFilteringReader(path string, idFilter *string, idSizeFilter *int) (*filteringReader, error) {
	opts := commitlog.NewReaderOptions(commitlog.NewOptions(), false)
	reader := commitlog.NewReader(opts)
	if _, err := reader.Open(path); err != nil {
		return nil, err
	}
	return &filteringReader{reader: reader, idFilter: idFilter, idSizeFilter: idSizeFilter}, nil
}

func (c *filteringReader) Read() (commitlog.LogEntry, bool, error) {
	for {
		entry, err := c.reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return commitlog.LogEntry{}, false, err
		}
		series := entry.Series
		if *c.idFilter != "" && !strings.Contains(series.ID.String(), *c.idFilter) {
			continue
		}
		if *c.idSizeFilter != 0 && len(series.ID.Bytes()) < *c.idSizeFilter {
			continue
		}
		return entry, true, nil
	}
	return commitlog.LogEntry{}, false, nil
}

func (c *filteringReader) Close() {
	if c != nil && c.reader != nil {
		if err := c.reader.Close(); err != nil {
			log.Fatalf("unable to close reader: %v", err)
		}
	}
}
