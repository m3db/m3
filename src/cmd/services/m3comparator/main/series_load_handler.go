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

package main

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

type idSeriesMap struct {
	start  time.Time
	end    time.Time
	series map[string]parser.Series
}

type nameIDSeriesMap map[string]idSeriesMap

type seriesLoadHandler struct {
	sync.RWMutex
	nameIDSeriesMap nameIDSeriesMap
	iterOpts        iteratorOptions
	iOpts           instrument.Options
}

// NewSeriesLoadHandler builds a handler that can load series
// to the comparator via an http endpoint.
func NewSeriesLoadHandler(
	iterOpts iteratorOptions, iOpts instrument.Options) http.Handler {
	return &seriesLoadHandler{
		iterOpts:        iterOpts,
		iOpts:           iOpts,
		nameIDSeriesMap: make(nameIDSeriesMap),
	}
}

func (l *seriesLoadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := l.iOpts.Logger()
	err := l.serveHTTP(r)
	if err != nil {
		logger.Error("unable to fetch data", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (l *seriesLoadHandler) getSeriesIterators(
	name string,
) encoding.SeriesIterators {
	l.RLock()
	defer l.RUnlock()

	logger := l.iOpts.Logger()
	seriesMap, found := l.nameIDSeriesMap[name]
	if !found || len(seriesMap.series) == 0 {
		return nil
	}

	iters := make([]encoding.SeriesIterator, 0, len(seriesMap.series))
	for _, series := range seriesMap.series {
		encoder := l.iterOpts.encoderPool.Get()
		dps := series.Datapoints
		encoder.Reset(time.Time{}, len(dps), nil)
		for _, dp := range dps {

			err := encoder.Encode(ts.Datapoint{
				Timestamp:      dp.Timestamp,
				Value:          dp.Value,
				TimestampNanos: xtime.ToUnixNano(dp.Timestamp),
			}, xtime.Second, nil)

			if err != nil {
				encoder.Close()
				logger.Error("error encoding datapoints", zap.Error(err))
				return nil
			}
		}

		readers := [][]xio.BlockReader{{{
			SegmentReader: xio.NewSegmentReader(encoder.Discard()),
			Start:         series.Start,
			BlockSize:     series.End.Sub(series.Start),
		}}}

		multiReader := encoding.NewMultiReaderIterator(
			iterAlloc,
			l.iterOpts.iteratorPools.MultiReaderIterator(),
		)

		sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(readers)
		multiReader.ResetSliceOfSlices(sliceOfSlicesIter, nil)
		tagIter, id := buildTagIteratorAndID(tagMap(series.Tags), l.iterOpts.tagOptions)
		iters = append(iters, encoding.NewSeriesIterator(
			encoding.SeriesIteratorOptions{
				ID:             id,
				Namespace:      ident.StringID("ns"),
				Tags:           tagIter,
				StartInclusive: xtime.ToUnixNano(series.Start),
				EndExclusive:   xtime.ToUnixNano(series.End),
				Replicas: []encoding.MultiReaderIterator{
					multiReader,
				},
			}, nil))
	}

	return encoding.NewSeriesIterators(
		iters,
		l.iterOpts.iteratorPools.MutableSeriesIterators(),
	)
}

func (l *seriesLoadHandler) serveHTTP(r *http.Request) error {
	l.Lock()
	defer l.Unlock()

	logger := l.iOpts.Logger()
	if err := r.ParseForm(); err != nil {
		return err
	}

	series := r.Form["series"]
	if len(series) != 1 {
		logger.Info("wrong length in series")
		return nil
	}

	buf := []byte(series[0])
	seriesList := make([]parser.Series, 0, 10)
	if err := json.Unmarshal(buf, &series); err != nil {
		logger.Error("could not unmarshal queries", zap.Error(err))
		return err
	}

	// NB: keep consistent start/end for the entire ingested set.
	start, end := time.Time{}, time.Time{}
	for _, series := range seriesList {
		if series.Start.IsZero() || series.Start.Before(start) {
			start = series.Start
		}

		if series.End.IsZero() || series.End.Before(start) {
			end = series.End
		}
	}

	if l.nameIDSeriesMap == nil {
		l.nameIDSeriesMap = make(nameIDSeriesMap, len(series))
	}

	name := string(l.iterOpts.tagOptions.MetricName())
	for _, series := range seriesList {
		name, found := series.Tags[name]
		if !found || len(series.Datapoints) == 0 {
			continue
		}

		// NB: overwrite existing series here.
		seriesMap := idSeriesMap{
			series: make(map[string]parser.Series, len(seriesList)),
		}
		id := series.GetOrGenID()
		logger.Info("setting series",
			zap.String("name", name), zap.String("id", id))

		series.Start = start
		series.End = end
		seriesMap.series[id] = series
	}

	return nil
}
