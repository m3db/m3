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
	"errors"
	golog "log"
	"math"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/m3ninx/search/executor"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/pborman/getopt"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/zap"
)

var (
	halfCPUs     = int(math.Max(float64(runtime.NumCPU()/2), 1))
	endlineBytes = []byte("\n")
)

func main() {

	var (
		optPathPrefix  = getopt.StringLong("path-prefix", 'p', "/var/lib/m3db", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace   = getopt.StringLong("namespace", 'n', "", "Namespace to query")
		optQuery       = getopt.StringLong("query", 'q', "", "Query to issue to match time series (PromQL selector)")
		optConcurrency = getopt.IntLong("concurrency", 'c', halfCPUs, "Query concurrency")
		optValidate    = true
	)
	getopt.BoolVarLong(&optValidate, "validate", 'v', "Validate index segments before querying")
	getopt.Parse()

	logConfig := zap.NewDevelopmentConfig()
	log, err := logConfig.Build()
	if err != nil {
		golog.Fatalf("unable to create logger: %+v", err)
	}

	if *optNamespace == "" || *optQuery == "" {
		getopt.Usage()
		os.Exit(1)
	}

	run(runOptions{
		filePathPrefix: *optPathPrefix,
		namespace:      *optNamespace,
		query:          *optQuery,
		validate:       optValidate,
		concurrency:    *optConcurrency,
		log:            log,
	})

}

type runOptions struct {
	filePathPrefix string
	namespace      string
	query          string
	validate       bool
	concurrency    int
	log            *zap.Logger
}

func run(opts runOptions) {
	log := opts.log

	parseOpts := promql.NewParseOptions()
	parse := parseOpts.MetricSelectorFn()

	matchers, err := parse(opts.query)
	if err != nil {
		log.Fatal("could not create matchers", zap.Error(err))
	}

	labelMatchers, err := toLabelMatchers(matchers)
	if err != nil {
		log.Fatal("could not create label matchers", zap.Error(err))
	}

	query, err := storage.PromReadQueryToM3(&prompb.Query{
		Matchers:         labelMatchers,
		StartTimestampMs: 0,
		EndTimestampMs:   time.Now().UnixNano() / int64(time.Millisecond),
	})
	if err != nil {
		log.Fatal("could not create M3 fetch query", zap.Error(err))
	}

	indexQuery, err := storage.FetchQueryToM3Query(query, storage.NewFetchOptions())
	if err != nil {
		log.Fatal("could not create M3 index query", zap.Error(err))
	}

	fsOpts := fs.NewOptions().
		SetFilePathPrefix(opts.filePathPrefix)

	if opts.validate {
		// Validate checksums before reading and/or validating contents if set.
		fsOpts = fsOpts.SetIndexReaderAutovalidateIndexSegments(true)
	}

	var (
		nsID      = ident.StringID(opts.namespace)
		infoFiles = fs.ReadIndexInfoFiles(fsOpts.FilePathPrefix(), nsID,
			fsOpts.InfoReaderBufferSize())
		results     = make(map[string]struct{})
		resultsLock sync.Mutex
		wg          sync.WaitGroup
	)

	log.Info("starting query",
		zap.Int("concurrency", opts.concurrency),
		zap.Bool("validateSegments", opts.validate))

	workers := xsync.NewWorkerPool(opts.concurrency)
	workers.Init()

	for _, infoFile := range infoFiles {
		if err := infoFile.Err.Error(); err != nil {
			log.Error("unable to read index info file",
				zap.Stringer("namespace", nsID),
				zap.Error(err),
				zap.String("filepath", infoFile.Err.Filepath()),
			)
			continue
		}

		readResult, err := fs.ReadIndexSegments(fs.ReadIndexSegmentsOptions{
			ReaderOptions: fs.IndexReaderOpenOptions{
				Identifier:  infoFile.ID,
				FileSetType: persist.FileSetFlushType,
			},
			FilesystemOptions: fsOpts,
		})
		if err != nil {
			log.Fatal("unable to read segments from index fileset", zap.Error(err))
			return
		}

		wg.Add(1)
		workers.Go(func() {
			defer wg.Done()

			var readers []index.Reader
			for _, seg := range readResult.Segments {
				reader, err := seg.Reader()
				if err != nil {
					log.Fatal("segment reader error", zap.Error(err))
				}

				readers = append(readers, reader)
			}

			executor := executor.NewExecutor(readers)

			iter, err := executor.Execute(indexQuery.Query.SearchQuery())
			if err != nil {
				log.Fatal("search execute error", zap.Error(err))
			}

			reader := docs.NewEncodedDocumentReader()
			fields := make(map[string]string)
			for iter.Next() {
				d := iter.Current()
				m, err := docs.MetadataFromDocument(d, reader)
				if err != nil {
					log.Fatal("error retrieve document metadata", zap.Error(err))
				}

				key := string(m.ID)

				resultsLock.Lock()
				_, ok := results[key]
				if !ok {
					results[key] = struct{}{}
				}
				resultsLock.Unlock()

				if ok {
					continue // Already printed.
				}

				for k := range fields {
					delete(fields, k)
				}
				for _, field := range m.Fields { // nolint:gocritic
					fields[string(field.Name)] = string(field.Value)
				}

				log.Info("matched document",
					zap.String("id", key),
					zap.Any("fields", fields))
			}

			if err := iter.Err(); err != nil {
				log.Fatal("iterate err", zap.Error(err))
			}
			if err := iter.Close(); err != nil {
				log.Fatal("iterate close err", zap.Error(err))
			}
		})
	}

	wg.Wait()
}

func toLabelMatchers(matchers []*labels.Matcher) ([]*prompb.LabelMatcher, error) {
	pbMatchers := make([]*prompb.LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		var mType prompb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			mType = prompb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			mType = prompb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			mType = prompb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			mType = prompb.LabelMatcher_NRE
		default:
			return nil, errors.New("invalid matcher type")
		}
		pbMatchers = append(pbMatchers, &prompb.LabelMatcher{
			Type:  mType,
			Name:  []byte(m.Name),
			Value: []byte(m.Value),
		})
	}
	return pbMatchers, nil
}
