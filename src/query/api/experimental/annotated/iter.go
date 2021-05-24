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

package annotated

import (
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

var defaultValue = ingest.IterValue{
	Tags:       models.EmptyTags(),
	Attributes: ts.DefaultSeriesAttributes(),
	Metadata:   ts.Metadata{},
}

type datapoint struct {
	ts.Datapoint

	annotation []byte
}

func sampleToDatapoint(s prompb.AnnotatedSample) datapoint {
	return datapoint{
		Datapoint: ts.Datapoint{
			Timestamp: xtime.UnixNano(s.Timestamp),
			Value:     s.Value,
		},
		annotation: s.Annotation,
	}
}

var _ ingest.DownsampleAndWriteIter = &iter{}

type iter struct {
	idx        int
	tags       []models.Tags
	datapoints []datapoint
	metadatas  []ts.Metadata
}

func newIter(
	timeseries []prompb.AnnotatedTimeSeries, tagOpts models.TagOptions,
) *iter {
	var (
		tags       = make([]models.Tags, 0, len(timeseries))
		datapoints = make([]datapoint, 0, len(timeseries))
	)
	for _, ts := range timeseries {
		t := storage.PromLabelsToM3Tags(ts.Labels, tagOpts)
		for _, s := range ts.Samples {
			tags = append(tags, t)
			datapoints = append(datapoints, sampleToDatapoint(s))
		}
	}

	return &iter{
		idx:        -1,
		tags:       tags,
		datapoints: datapoints,
	}
}

func (i *iter) Next() bool {
	i.idx++
	return i.idx < len(i.tags)
}

func (i *iter) Current() ingest.IterValue {
	if len(i.tags) == 0 || i.idx < 0 || i.idx >= len(i.tags) {
		return defaultValue
	}
	curr := i.datapoints[i.idx]
	value := ingest.IterValue{
		Tags:       i.tags[i.idx],
		Datapoints: ts.Datapoints{curr.Datapoint},
		Attributes: ts.DefaultSeriesAttributes(),
		Unit:       xtime.Millisecond,
		Annotation: curr.annotation,
	}
	if i.idx < len(i.metadatas) {
		value.Metadata = i.metadatas[i.idx]
	}
	return value
}

func (i *iter) Reset() error {
	i.idx = -1
	return nil
}

func (i *iter) Error() error {
	return nil
}

func (i *iter) SetCurrentMetadata(metadata ts.Metadata) {
	if len(i.metadatas) == 0 {
		i.metadatas = make([]ts.Metadata, len(i.tags))
	}
	if i.idx < 0 || i.idx >= len(i.metadatas) {
		return
	}
	i.metadatas[i.idx] = metadata
}
