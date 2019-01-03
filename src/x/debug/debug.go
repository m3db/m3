// Copyright (c) 2019 Uber Technologies, Inc.
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

package debug

import (
	"archive/zip"
	"fmt"
	"io"
)

// Source provides functions for fething debug information from a single debug source.
type Source interface {
	// Write writes it's debug information into the provided writer
	Write(w io.Writer) error
}

// ZipWriter aggregates sources and writes them in a zip file.
type ZipWriter interface {
	// WriteZip writes a ZIP file in the provided writer.
	// The archive contains the dumps of all sources in separate files.
	WriteZip(io.Writer) error
	// RegisterSource adds a new source to the produced archive.
	RegisterSource(string, Source) error
}

type zipWriter struct {
	sources map[string]Source
}

// NewZipWriter returns an instance of an ZipWriter.
func NewZipWriter() ZipWriter {
	return &zipWriter{
		sources: make(map[string]Source),
	}
}

// RegisterSource adds a new source in the ZipWriter instance.
// It will return an error if a source with the same filename exists.
func (i *zipWriter) RegisterSource(dumpFileName string, p Source) error {
	if _, ok := i.sources[dumpFileName]; ok {
		return fmt.Errorf("dumpfile already registered %s", dumpFileName)
	}
	i.sources[dumpFileName] = p
	return nil
}

// WriteZip writes a ZIP file with the data from all sources in the given writer.
// It will return an error if any of the sources fail to write their data.
func (i *zipWriter) WriteZip(w io.Writer) error {
	zw := zip.NewWriter(w)
	defer zw.Close()

	for filename, p := range i.sources {
		fw, err := zw.Create(filename)
		if err != nil {
			return err
		}
		err = p.Write(fw)
		if err != nil {
			return err
		}
	}
	return nil
}
