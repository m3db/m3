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
	"io"

	"github.com/m3db/m3/src/x/unsafe"

	"fmt"
	golog "log"
	"os"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/query/util/json"
	"github.com/m3db/m3/src/x/ident"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

func main() {
	var (
		optPathPrefix = getopt.StringLong("path-prefix", 'p', "/var/lib/m3db", "Path prefix [e.g. /var/lib/m3db]")
		optOutputFile = getopt.StringLong("output-file", 'o', "", "Output JSON file of line delimited JSON objects for each segment")
	)
	getopt.Parse()

	logConfig := zap.NewDevelopmentConfig()
	log, err := logConfig.Build()
	if err != nil {
		golog.Fatalf("unable to create logger: %+v", err)
	}

	if *optPathPrefix == "" || *optOutputFile == "" {
		getopt.Usage()
		os.Exit(1)
	}

	run(runOptions{
		filePathPrefix: *optPathPrefix,
		outputFilePath: *optOutputFile,
		log:            log,
	})

}

type runOptions struct {
	filePathPrefix string
	outputFilePath string
	log            *zap.Logger
}

func run(opts runOptions) {
	log := opts.log

	fsOpts := fs.NewOptions().
		SetFilePathPrefix(opts.filePathPrefix)

	indexDirPath := fs.IndexDataDirPath(opts.filePathPrefix)

	namespaces, err := dirFiles(indexDirPath)
	if err != nil {
		log.Fatal("could not read namespaces", zap.Error(err))
	}

	// Get all fileset files.
	log.Info("discovered namespaces", zap.Strings("namespaces", namespaces))

	out, err := os.Create(opts.outputFilePath)
	if err != nil {
		log.Fatal("unable to create output file",
			zap.String("file", opts.outputFilePath),
			zap.Error(err))
	}

	for _, namespace := range namespaces {
		log.Info("reading segments", zap.String("namespace", namespace))
		ns := ident.StringID(namespace)

		readNamespaceSegments(out, ns, fsOpts, log)

		// Separate by endline.
		if _, err := out.WriteString("\n"); err != nil {
			log.Fatal("could not write endline", zap.Error(err))
		}
	}
}

func readNamespaceSegments(
	out io.Writer,
	nsID ident.ID,
	fsOpts fs.Options,
	log *zap.Logger,
) {
	infoFiles := fs.ReadIndexInfoFiles(fsOpts.FilePathPrefix(), nsID,
		fsOpts.InfoReaderBufferSize())

	for _, infoFile := range infoFiles {
		if err := infoFile.Err.Error(); err != nil {
			log.Error("unable to read index info file",
				zap.Stringer("namespace", nsID),
				zap.Error(err),
				zap.String("filepath", infoFile.Err.Filepath()),
			)
			continue
		}

		segments, err := fs.ReadIndexSegments(fs.ReadIndexSegmentsOptions{
			ReaderOptions: fs.IndexReaderOpenOptions{
				Identifier:  infoFile.ID,
				FileSetType: persist.FileSetFlushType,
			},
			FilesystemOptions: fsOpts,
		})
		if err != nil {
			log.Error("unable to read segments from index fileset",
				zap.Stringer("namespace", nsID),
				zap.Error(err),
				zap.Time("blockStart", time.Unix(0, infoFile.Info.BlockStart)),
				zap.Int("volumeIndex", infoFile.ID.VolumeIndex),
			)
			continue
		}

		for i, seg := range segments {
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
