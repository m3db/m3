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

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	golog "log"
	"math"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/query/util/json"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	"github.com/m3db/m3/src/x/unsafe"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

var (
	halfCPUs     = int(math.Max(float64(runtime.NumCPU()/2), 1))
	endlineBytes = []byte("\n")
)

func main() {
	var (
		optPathPrefix          = getopt.StringLong("path-prefix", 'p', "/var/lib/m3db", "Path prefix [e.g. /var/lib/m3db]")
		optOutputFile          = getopt.StringLong("output-file", 'o', "", "Output JSON file of line delimited JSON objects for each segment")
		optValidate            = getopt.BoolLong("validate", 'v', "Validate the segments, do not print out metadata")
		optValidateConcurrency = getopt.IntLong("validate-concurrency", 'c', halfCPUs, "Validation concurrency")
	)
	getopt.Parse()

	logConfig := zap.NewDevelopmentConfig()
	log, err := logConfig.Build()
	if err != nil {
		golog.Fatalf("unable to create logger: %+v", err)
	}

	if *optOutputFile != "" && *optValidate {
		log.Error("cannot write output and validate, do not set output file if validating")
		getopt.Usage()
		os.Exit(1)
	}

	if *optPathPrefix == "" || (*optOutputFile == "" && !*optValidate) {
		getopt.Usage()
		os.Exit(1)
	}

	run(runOptions{
		filePathPrefix:      *optPathPrefix,
		outputFilePath:      *optOutputFile,
		validate:            *optValidate,
		validateConcurrency: *optValidateConcurrency,
		log:                 log,
	})
}

type runOptions struct {
	filePathPrefix      string
	outputFilePath      string
	validate            bool
	validateConcurrency int
	log                 *zap.Logger
}

func run(opts runOptions) {
	log := opts.log

	fsOpts := fs.NewOptions().
		SetFilePathPrefix(opts.filePathPrefix).
		// Always validate checksums before reading and/or validating contents
		// regardless of whether this is a validation run or just reading
		// the raw files.
		SetIndexReaderAutovalidateIndexSegments(true)

	indexDirPath := fs.IndexDataDirPath(opts.filePathPrefix)

	namespaces, err := dirFiles(indexDirPath)
	if err != nil {
		log.Fatal("could not read namespaces", zap.Error(err))
	}

	// Get all fileset files.
	log.Info("discovered namespaces", zap.Strings("namespaces", namespaces))

	var (
		out                io.Writer
		validateWorkerPool xsync.WorkerPool
	)
	if opts.validate {
		// Only validating, output to dev null.
		out = ioutil.Discard
		validateWorkerPool = xsync.NewWorkerPool(opts.validateConcurrency)
		validateWorkerPool.Init()
		log.Info("validating segment files",
			zap.Int("concurrency", opts.validateConcurrency))
	} else {
		// Output to file.
		out, err = os.Create(opts.outputFilePath)
		if err != nil {
			log.Fatal("unable to create output file",
				zap.String("file", opts.outputFilePath),
				zap.Error(err))
		}
		log.Info("writing output JSON line delimited",
			zap.String("path", opts.outputFilePath))
	}

	for _, namespace := range namespaces {
		log.Info("reading segments", zap.String("namespace", namespace))
		ns := ident.StringID(namespace)

		readNamespaceSegments(out, opts.validate, validateWorkerPool,
			ns, fsOpts, log)

		// Separate by endline.
		if _, err := out.Write(endlineBytes); err != nil {
			log.Fatal("could not write endline", zap.Error(err))
		}
	}
}

func readNamespaceSegments(
	out io.Writer,
	validate bool,
	validateWorkerPool xsync.WorkerPool,
	nsID ident.ID,
	fsOpts fs.Options,
	log *zap.Logger,
) {
	var (
		infoFiles = fs.ReadIndexInfoFiles(fs.ReadIndexInfoFilesOptions{
			FilePathPrefix:   fsOpts.FilePathPrefix(),
			Namespace:        nsID,
			ReaderBufferSize: fsOpts.InfoReaderBufferSize(),
		})
		wg sync.WaitGroup
	)

	for _, infoFile := range infoFiles {
		if err := infoFile.Err.Error(); err != nil {
			log.Error("unable to read index info file",
				zap.Stringer("namespace", nsID),
				zap.Error(err),
				zap.String("filepath", infoFile.Err.Filepath()),
			)
			continue
		}

		if !validate {
			readBlockSegments(out, nsID, infoFile, fsOpts, log)
			continue
		}

		// Validating, so use validation concurrency.
		wg.Add(1)
		validateWorkerPool.Go(func() {
			defer wg.Done()
			readBlockSegments(out, nsID, infoFile, fsOpts, log)
		})
	}

	// Wait for any concurrent validation.
	wg.Wait()
}

func readBlockSegments(
	out io.Writer,
	nsID ident.ID,
	infoFile fs.ReadIndexInfoFileResult,
	fsOpts fs.Options,
	log *zap.Logger,
) {
	// Make sure if we fatal or error out the exact block is known.
	log = log.With(
		zap.String("namespace", nsID.String()),
		zap.String("blockStart", infoFile.ID.BlockStart.String()),
		zap.Int64("blockStartUnixNano", infoFile.ID.BlockStart.UnixNano()),
		zap.Int("volumeIndex", infoFile.ID.VolumeIndex),
		zap.Strings("files", infoFile.AbsoluteFilePaths))

	log.Info("reading block segments")

	readResult, err := fs.ReadIndexSegments(fs.ReadIndexSegmentsOptions{
		ReaderOptions: fs.IndexReaderOpenOptions{
			Identifier:  infoFile.ID,
			FileSetType: persist.FileSetFlushType,
		},
		FilesystemOptions: fsOpts,
	})
	if err != nil {
		log.Error("unable to read segments from index fileset", zap.Error(err))
		return
	}

	if readResult.Validated {
		log.Info("validated segments")
	} else {
		log.Error("expected to validate segments but did not validate")
	}

	for i, seg := range readResult.Segments {
		jw := json.NewWriter(out)
		jw.BeginObject()

		jw.BeginObjectField("namespace")
		jw.WriteString(nsID.String())

		jw.BeginObjectField("blockStart")
		jw.WriteString(time.Unix(0, infoFile.Info.BlockStart).Format(time.RFC3339))

		jw.BeginObjectField("volumeIndex")
		jw.WriteInt(infoFile.ID.VolumeIndex)

		jw.BeginObjectField("segmentIndex")
		jw.WriteInt(i)

		reader, err := seg.Reader()
		if err != nil {
			log.Fatal("unable to create segment reader", zap.Error(err))
		}

		iter, err := reader.AllDocs()
		if err != nil {
			log.Fatal("unable to iterate segment docs", zap.Error(err))
		}

		jw.BeginObjectField("documents")
		jw.BeginArray()
		for postingsID := 0; iter.Next(); postingsID++ {
			d := iter.Current()
			jw.BeginObject()

			jw.BeginObjectField("postingsID")
			jw.WriteInt(postingsID)

			jw.BeginObjectField("id")
			unsafe.WithString(d.ID, func(str string) {
				jw.WriteString(str)
			})

			jw.BeginObjectField("fields")

			jw.BeginArray()
			for _, field := range d.Fields {
				jw.BeginObject()

				jw.BeginObjectField("name")
				unsafe.WithString(field.Name, func(str string) {
					jw.WriteString(str)
				})

				jw.BeginObjectField("value")
				unsafe.WithString(field.Name, func(str string) {
					jw.WriteString(str)
				})

				jw.EndObject()
			}
			jw.EndArray()

			jw.EndObject()
		}
		jw.EndArray()

		if err := iter.Err(); err != nil {
			log.Fatal("doc iterator error", zap.Error(err))
		}
		if err := iter.Close(); err != nil {
			log.Fatal("doc iterator close error", zap.Error(err))
		}

		fieldsIter, err := seg.FieldsIterable().Fields()
		if err != nil {
			log.Fatal("could not create fields iterator", zap.Error(err))
		}

		jw.BeginObjectField("fields")
		jw.BeginArray()
		for fieldsIter.Next() {
			field := fieldsIter.Current()

			jw.BeginObject()
			jw.BeginObjectField("field")
			unsafe.WithString(field, func(str string) {
				jw.WriteString(str)
			})

			termsIter, err := seg.TermsIterable().Terms(field)
			if err != nil {
				log.Fatal("could not create terms iterator", zap.Error(err))
			}

			jw.BeginObjectField("terms")
			jw.BeginArray()
			for termsIter.Next() {
				term, postingsList := termsIter.Current()

				jw.BeginObject()
				jw.BeginObjectField("term")
				unsafe.WithString(term, func(str string) {
					jw.WriteString(str)
				})

				postingsIter := postingsList.Iterator()

				jw.BeginObjectField("postings")
				jw.BeginArray()
				for postingsIter.Next() {
					postingsID := postingsIter.Current()
					jw.WriteInt(int(postingsID))
				}
				jw.EndArray()
				jw.EndObject()

				if err := postingsIter.Err(); err != nil {
					log.Fatal("postings iterator error", zap.Error(err))
				}

				if err := postingsIter.Close(); err != nil {
					log.Fatal("postings iterator close error", zap.Error(err))
				}
			}
			jw.EndArray()
			jw.EndObject()

			if err := termsIter.Err(); err != nil {
				log.Fatal("field iterator error", zap.Error(err))
			}

			if err := termsIter.Close(); err != nil {
				log.Fatal("field iterator close error", zap.Error(err))
			}
		}
		jw.EndArray()

		if err := fieldsIter.Err(); err != nil {
			log.Fatal("field iterator error", zap.Error(err))
		}

		if err := fieldsIter.Close(); err != nil {
			log.Fatal("field iterator close error", zap.Error(err))
		}

		jw.EndObject()

		if err := jw.Flush(); err != nil {
			log.Fatal("could not flush JSON writer", zap.Error(err))
		}
		if err := jw.Close(); err != nil {
			log.Fatal("could not close JSON writer", zap.Error(err))
		}
	}
}

func dirFiles(dirPath string) ([]string, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, fmt.Errorf("could not open dir: %v", err)
	}

	defer dir.Close()

	stat, err := dir.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not stat dir: %v", err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", dirPath)
	}

	entries, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, fmt.Errorf("could not read dir names: %v", err)
	}

	results := entries[:0]
	for _, p := range entries {
		if p == "." || p == ".." || p == "./.." || p == "./" || p == "../" || p == "./../" {
			continue
		}
		results = append(results, p)
	}
	return results, nil
}
