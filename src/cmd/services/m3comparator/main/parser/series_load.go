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

package parser

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

// Options are options for series parsing.
type Options struct {
	EncoderPool       encoding.EncoderPool
	IteratorPools     encoding.IteratorPools
	TagOptions        models.TagOptions
	InstrumentOptions instrument.Options
	Size              int
}

type nameIDSeriesMap map[string]idSeriesMap

type idSeriesMap struct {
	start  time.Time
	end    time.Time
	series map[string][]Series
}

type seriesReader struct {
	iterOpts        Options
	nameIDSeriesMap nameIDSeriesMap
	sync.RWMutex
}

// SeriesReader reads SeriesIterators from a generic io.Reader.
type SeriesReader interface {
	SeriesIterators(name string) (encoding.SeriesIterators, error)
	Load(reader io.Reader) error
	Clear()
}

// NewSeriesReader creates a new SeriesReader that reads entries as
// a slice of Series.
func NewSeriesReader(opts Options) SeriesReader {
	size := 10
	if opts.Size != 0 {
		size = opts.Size
	}

	return &seriesReader{
		iterOpts:        opts,
		nameIDSeriesMap: make(nameIDSeriesMap, size),
	}
}

func (l *seriesReader) SeriesIterators(name string) (encoding.SeriesIterators, error) {
	l.RLock()
	defer l.RUnlock()

	var seriesMaps []idSeriesMap
	logger := l.iterOpts.InstrumentOptions.Logger()
	if name == "" {
		// return all preloaded data
		seriesMaps = make([]idSeriesMap, 0, len(l.nameIDSeriesMap))
		for _, series := range l.nameIDSeriesMap {
			seriesMaps = append(seriesMaps, series)
		}
	} else {
		seriesMap, found := l.nameIDSeriesMap[name]
		if !found || len(seriesMap.series) == 0 {
			return nil, nil
		}

		seriesMaps = append(seriesMaps, seriesMap)
	}

	iters := make([]encoding.SeriesIterator, 0, len(seriesMaps))
	for _, seriesMap := range seriesMaps {
		for _, seriesPerID := range seriesMap.series {
			for _, series := range seriesPerID {
				encoder := l.iterOpts.EncoderPool.Get()
				dps := series.Datapoints
				startTime := time.Time{}
				if len(dps) > 0 {
					startTime = dps[0].Timestamp.Truncate(time.Hour)
				}

				encoder.Reset(xtime.ToUnixNano(startTime), len(dps), nil)
				for _, dp := range dps {
					err := encoder.Encode(ts.Datapoint{
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
					Start:         xtime.ToUnixNano(series.Start),
					BlockSize:     series.End.Sub(series.Start),
				}}}

				multiReader := encoding.NewMultiReaderIterator(
					iterAlloc,
					l.iterOpts.IteratorPools.MultiReaderIterator(),
				)

				sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(readers)
				multiReader.ResetSliceOfSlices(sliceOfSlicesIter, nil)

				tagIter, id := buildTagIteratorAndID(series.Tags, l.iterOpts.TagOptions)
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
		}
	}

	return encoding.NewSeriesIterators(
		iters,
		l.iterOpts.IteratorPools.MutableSeriesIterators(),
	), nil
}

func calculateSeriesRange(seriesList []Series) (time.Time, time.Time) {
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

func (l *seriesReader) Load(reader io.Reader) error {
	l.Lock()
	defer l.Unlock()

	buf, err := ioutil.ReadAll(reader)
	logger := l.iterOpts.InstrumentOptions.Logger()
	if err != nil {
		logger.Error("could not read body", zap.Error(err))
		return err
	}

	seriesList := make([]Series, 0, 10)
	if err := json.Unmarshal(buf, &seriesList); err != nil {
		logger.Error("could not unmarshal queries", zap.Error(err))
		return err
	}

	// NB: keep consistent start/end for the entire ingested set.
	start, end := calculateSeriesRange(seriesList)
	nameKey := string(l.iterOpts.TagOptions.MetricName())
	nameMap := make(nameIDSeriesMap, len(seriesList))
	for _, series := range seriesList {
		names := series.Tags.Get(nameKey)
		if len(names) != 1 || len(series.Datapoints) == 0 {
			if len(names) > 1 {
				err := fmt.Errorf("series has duplicate __name__ tags: %v", names)
				logger.Error("bad __name__ variable", zap.Error(err))
				return err
			}

			continue
		}

		name := names[0]
		seriesMap, found := nameMap[name]
		if !found {
			seriesMap = idSeriesMap{
				series: make(map[string][]Series, len(seriesList)),
			}
		}

		id := series.IDOrGenID()
		seriesList, found := seriesMap.series[id]
		if !found {
			seriesList = make([]Series, 0, 1)
		} else {
			logger.Info("duplicate tag set in loaded series",
				zap.Int("count", len(seriesList)),
				zap.String("id", id))
		}

		seriesList = append(seriesList, series)
		seriesMap.series[id] = seriesList
		logger.Info("setting series",
			zap.String("name", name), zap.String("id", id))

		series.Start = start
		series.End = end
		nameMap[name] = seriesMap
	}

	for k, v := range nameMap {
		// NB: overwrite existing series.
		l.nameIDSeriesMap[k] = v
	}

	return nil
}

func (l *seriesReader) Clear() {
	l.Lock()
	for k := range l.nameIDSeriesMap {
		delete(l.nameIDSeriesMap, k)
	}

	l.Unlock()
}
