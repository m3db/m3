// Copyright (c) 2018 Uber Technologies, Inc.
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

package debugdump

import (
	"archive/zip"
	"fmt"
	"io"
)

// DataProvider returns an instance of a provider for debug information
type DataProvider interface {
	// ProvideData writes it's debug information into the provided writer
	ProvideData(w io.Writer) error
}

// DataDumper represents a collection of InfoProviders
type DataDumper interface {
	// Dump writes a ZIP file in the provided writer
	// the archive contains the dumps of all the providers in separate files
	DumpData(io.Writer) error
	// RegisterProvider adds a new provider the aggregator
	RegisterProvider(string, DataProvider) error
}

type dataDumper struct {
	providers map[string]DataProvider
}

// NewDataDumper returns an instance of an DataDumper
func NewDataDumper() DataDumper {
	return &dataDumper{
		providers: make(map[string]DataProvider),
	}
}

func (i *dataDumper) RegisterProvider(dumpFileName string, p DataProvider) error {
	if _, ok := i.providers[dumpFileName]; ok {
		return fmt.Errorf("dumpfile already registered %s", dumpFileName)
	}
	i.providers[dumpFileName] = p
	return nil
}

func (i *dataDumper) DumpData(w io.Writer) error {
	zw := zip.NewWriter(w)
	defer zw.Close()

	for filename, p := range i.providers {
		fw, err := zw.Create(filename)
		if err != nil {
			return err
		}
		err = p.ProvideData(fw)
		if err != nil {
			return err
		}
	}
	return nil
}
