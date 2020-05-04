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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/ident"
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
}

var _ http.Handler = (*seriesLoadHandler)(nil)

// newSeriesLoadHandler builds a handler that can load series
// to the comparator via an http endpoint.
func newSeriesLoadHandler(iterOpts iteratorOptions) *seriesLoadHandler {
	return &seriesLoadHandler{
		iterOpts:        iterOpts,
		nameIDSeriesMap: make(nameIDSeriesMap),
	}
}

func (l *seriesLoadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := l.iterOpts.iOpts.Logger()
	err := l.serveHTTP(r)
	if err != nil {
		logger.Error("unable to fetch data", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (l *seriesLoadHandler) getSeriesIterators(
	name string) (encoding.SeriesIterators, error) {
	l.RLock()
	defer l.RUnlock()

	logger := l.iterOpts.iOpts.Logger()
	seriesMap, found := l.nameIDSeriesMap[name]
	if !found || len(seriesMap.series) == 0 {
		return nil, nil
	}

	iters := make([]encoding.SeriesIterator, 0, len(seriesMap.series))
	for _, series := range seriesMap.series {
		encoder := l.iterOpts.encoderPool.Get()
		dps := series.Datapoints
		startTime := time.Time{}
		if len(dps) > 0 {
			startTime = dps[0].Timestamp.Truncate(time.Hour)
		}

		encoder.Reset(startTime, len(dps), nil)
		for _, dp := range dps {
			err := encoder.Encode(ts.Datapoint{
				Timestamp:      dp.Timestamp,
				Value:          float64(dp.Value),
				TimestampNanos: xtime.ToUnixNano(dp.Timestamp),
			}, xtime.Nanosecond, nil)

			if err != nil {
				encoder.Close()
				logger.Error("error encoding datapoints", zap.Error(err))
				return nil, err
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
		iter := encoding.NewSeriesIterator(
			encoding.SeriesIteratorOptions{
				ID:             id,
				Namespace:      ident.StringID("ns"),
				Tags:           tagIter,
				StartInclusive: xtime.ToUnixNano(series.Start),
				EndExclusive:   xtime.ToUnixNano(series.End),
				Replicas: []encoding.MultiReaderIterator{
					multiReader,
				},
			}, nil)

		iters = append(iters, iter)
	}

	return encoding.NewSeriesIterators(
		iters,
		l.iterOpts.iteratorPools.MutableSeriesIterators(),
	), nil
}

func calculateSeriesRange(seriesList []parser.Series) (time.Time, time.Time) {
	// NB: keep consistent start/end for the entire ingested set.
	//
	// Try taking from set start/end; infer from first/last endpoint otherwise.
	start, end := time.Time{}, time.Time{}
	for _, series := range seriesList {
		if start.IsZero() || series.Start.Before(start) {
			start = series.Start
		}

		if end.IsZero() || series.End.Before(start) {
			end = series.End
		}
	}

	if !start.IsZero() && !end.IsZero() {
		return start, end
	}

	for _, series := range seriesList {
		dps := series.Datapoints
		if len(dps) == 0 {
			continue
		}

		first, last := dps[0].Timestamp, dps[len(dps)-1].Timestamp
		if start.IsZero() || first.Before(start) {
			start = first
		}

		if end.IsZero() || last.Before(start) {
			end = last
		}
	}

	return start, end
}

func (l *seriesLoadHandler) serveHTTP(r *http.Request) error {
	l.Lock()
	defer l.Unlock()

	if r.Method == http.MethodDelete {
		l.nameIDSeriesMap = make(map[string]idSeriesMap)
		return nil
	}

	logger := l.iterOpts.iOpts.Logger()
	body := r.Body
	defer body.Close()
	buf, err := ioutil.ReadAll(body)
	if err != nil {
		logger.Error("could not read body", zap.Error(err))
		return err
	}

	seriesList := make([]parser.Series, 0, 10)
	if err := json.Unmarshal(buf, &seriesList); err != nil {
		logger.Error("could not unmarshal queries", zap.Error(err))
		return err
	}

	// NB: keep consistent start/end for the entire ingested set.
	start, end := calculateSeriesRange(seriesList)
	name := string(l.iterOpts.tagOptions.MetricName())
	nameMap := make(nameIDSeriesMap, len(seriesList))
	for _, series := range seriesList {
		name, found := series.Tags[name]
		if !found || len(series.Datapoints) == 0 {
			continue
		}

		seriesMap, found := nameMap[name]
		if !found {
			seriesMap = idSeriesMap{
				series: make(map[string]parser.Series, len(seriesList)),
			}
		}

		id := series.IDOrGenID()
		logger.Info("setting series",
			zap.String("name", name), zap.String("id", id))

		series.Start = start
		series.End = end
		seriesMap.series[id] = series
		nameMap[name] = seriesMap
	}

	for k, v := range nameMap {
		// NB: overwrite existing series.
		l.nameIDSeriesMap[k] = v
	}

	return nil
}
