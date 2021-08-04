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
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// DebugURL is the url for the debug dump endpoint.
	DebugURL = "/debug/dump"
	// DebugMethod is the HTTP method.
	DebugMethod = http.MethodGet
)

// Source is the interface that must be implemented to provide a new debug
// source. Each debug source's Write method will be called to write out a debug
// file for that source into the overall debug zip file.
type Source interface {
	// Write writes it's debug information into the provided writer.
	Write(w io.Writer, r *http.Request) error
}

// ZipWriter aggregates sources and writes them in a zip file.
type ZipWriter interface {
	// WriteZip writes a ZIP file in the provided writer.
	// The archive contains the dumps of all sources in separate files.
	WriteZip(io.Writer, *http.Request) error
	// RegisterSource adds a new source to the produced archive.
	RegisterSource(fileName string, source Source) error
	// HTTPHandler sends out the ZIP file as raw bytes.
	HTTPHandler() http.Handler
	// RegisterHandler wires the HTTPHandlerFunc with the given router.
	RegisterHandler(handlerPath string, router *http.ServeMux) error
}

type zipWriter struct {
	sources map[string]Source
	logger  *zap.Logger
}

// NewZipWriter returns an instance of an ZipWriter. The passed prefix
// indicates the folder where to save the zip files.
func NewZipWriter(iopts instrument.Options) ZipWriter {
	return &zipWriter{
		sources: make(map[string]Source),
		logger:  iopts.Logger(),
	}
}

// NewZipWriterWithDefaultSources returns a zipWriter with the following
// debug sources already registered: CPU, heap, host, goroutines.
func NewZipWriterWithDefaultSources(
	cpuProfileDuration time.Duration,
	iopts instrument.Options,
) (ZipWriter, error) {
	zw := NewZipWriter(iopts)

	err := zw.RegisterSource("cpu.prof", NewCPUProfileSource(cpuProfileDuration))
	if err != nil {
		return nil, fmt.Errorf("unable to register CPUProfileSource: %s", err)
	}

	err = zw.RegisterSource("heap.prof", NewHeapDumpSource())
	if err != nil {
		return nil, fmt.Errorf("unable to register HeapDumpSource: %s", err)
	}

	err = zw.RegisterSource("host.json", NewHostInfoSource())
	if err != nil {
		return nil, fmt.Errorf("unable to register HostInfoSource: %s", err)
	}

	gp, err := NewProfileSource("goroutine", 2)
	if err != nil {
		return nil, fmt.Errorf("unable to create goroutineProfileSource: %s", err)
	}

	err = zw.RegisterSource("goroutine.prof", gp)
	return zw, nil
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
func (i *zipWriter) WriteZip(w io.Writer, r *http.Request) error {
	zw := zip.NewWriter(w)
	defer zw.Close()

	for filename, p := range i.sources {
		fw, err := zw.Create(filename)
		if err != nil {
			return err
		}
		err = p.Write(fw, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *zipWriter) HTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := bytes.NewBuffer([]byte{})
		if err := i.WriteZip(buf, r); err != nil {
			xhttp.WriteError(w, fmt.Errorf("unable to write ZIP file: %s", err))
			return
		}
		if _, err := io.Copy(w, buf); err != nil {
			i.logger.Error("unable to write ZIP response", zap.Error(err))
		}
	})
}

func (i *zipWriter) RegisterHandler(path string, r *http.ServeMux) error {
	r.Handle(path, i.HTTPHandler())
	return nil
}
