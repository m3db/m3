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

package influxdb

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"

	imodels "github.com/influxdata/influxdb/models"
	"go.uber.org/zap"

	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	// InfluxWriteURL is the Influx DB write handler URL
	InfluxWriteURL = route.Prefix + "/influxdb/write"

	// InfluxWriteHTTPMethod is the HTTP method used with this resource
	InfluxWriteHTTPMethod = http.MethodPost
)

var defaultValue = ingest.IterValue{
	Tags:       models.EmptyTags(),
	Attributes: ts.DefaultSeriesAttributes(),
	Metadata:   ts.Metadata{},
}

type ingestWriteHandler struct {
	handlerOpts  options.HandlerOptions
	tagOpts      models.TagOptions
	promRewriter *promRewriter
}

type ingestField struct {
	name  []byte // to be stored in __name__; rest of tags stay constant for the Point
	value float64
}

type ingestIterator struct {
	// what is being iterated (comes from outside)
	points       []imodels.Point
	tagOpts      models.TagOptions
	promRewriter *promRewriter

	// internal
	pointIndex int
	err        xerrors.MultiError
	metadatas  []ts.Metadata

	// following entries are within current point, and initialized
	// when we go to the first entry in the current point
	fields         []*ingestField
	nextFieldIndex int
	tags           models.Tags
}

func (ii *ingestIterator) populateFields() bool {
	point := ii.points[ii.pointIndex]
	it := point.FieldIterator()
	n := 0
	ii.fields = make([]*ingestField, 0, 10)
	bname := make([]byte, 0, len(point.Name())+1)
	bname = append(bname, point.Name()...)
	bname = append(bname, byte('_'))
	bnamelen := len(bname)
	ii.promRewriter.rewriteMetric(bname)
	for it.Next() {
		var value float64 = 0
		n += 1
		switch it.Type() {
		case imodels.Boolean:
			v, err := it.BooleanValue()
			if err != nil {
				ii.err = ii.err.Add(err)
				continue
			}
			if v {
				value = 1.0
			}
		case imodels.Integer:
			v, err := it.IntegerValue()
			if err != nil {
				ii.err = ii.err.Add(err)
				continue
			}
			value = float64(v)
		case imodels.Unsigned:
			v, err := it.UnsignedValue()
			if err != nil {
				ii.err = ii.err.Add(err)
				continue
			}
			value = float64(v)
		case imodels.Float:
			v, err := it.FloatValue()
			if err != nil {
				ii.err = ii.err.Add(err)
				continue
			}
			value = v
		default:
			// TBD if we should stick strings as
			// tags or not; to prevent cardinality
			// explosion, we drop them for now
			continue
		}
		tail := it.FieldKey()
		name := make([]byte, 0, bnamelen+len(tail))
		name = append(name, bname...)
		name = append(name, tail...)
		ii.promRewriter.rewriteMetricTail(name[bnamelen:])
		ii.fields = append(ii.fields, &ingestField{name: name, value: value})
	}
	return n > 0
}

func (ii *ingestIterator) Next() bool {
	for len(ii.points) > ii.pointIndex {
		if ii.nextFieldIndex == 0 {
			// Populate tags only if we have fields we care about
			if ii.populateFields() {
				point := ii.points[ii.pointIndex]
				ptags := point.Tags()
				tags := models.NewTags(len(ptags), ii.tagOpts)
				for _, tag := range ptags {
					name := make([]byte, len(tag.Key))
					copy(name, tag.Key)
					ii.promRewriter.rewriteLabel(name)
					tags = tags.AddTagWithoutNormalizing(models.Tag{Name: name, Value: tag.Value})
				}
				// sanity check no duplicate Name's;
				// after Normalize, they are sorted so
				// can just check them sequentially
				valid := true
				if len(tags.Tags) > 0 {
					// Dummy w/o value set; used for dupe check and value is rewrittein in-place in SetName later on
					tags = tags.AddTag(models.Tag{Name: tags.Opts.MetricName()})
					name := tags.Tags[0].Name
					for i := 1; i < len(tags.Tags); i++ {
						iname := tags.Tags[i].Name
						if bytes.Equal(name, iname) {
							ii.err = ii.err.Add(fmt.Errorf("non-unique Prometheus label %v", string(iname)))
							valid = false
							break
						}
						name = iname
					}
				}
				if !valid {
					ii.pointIndex += 1
					continue
				}
				ii.tags = tags
			}
		}
		ii.nextFieldIndex += 1
		if ii.nextFieldIndex > len(ii.fields) {
			ii.pointIndex += 1
			ii.nextFieldIndex = 0
			continue
		}
		return true
	}
	return false
}

func copyTagsWithNewName(t models.Tags, name []byte) models.Tags {
	copiedTags := make([]models.Tag, t.Len())
	metricName := t.Opts.MetricName()
	nameHandled := false
	for i, tag := range t.Tags {
		if !nameHandled && bytes.Equal(tag.Name, metricName) {
			copiedTags[i] = models.Tag{Name: metricName, Value: name}
			nameHandled = true
		} else {
			copiedTags[i] = tag
		}
	}
	return models.Tags{Tags: copiedTags, Opts: t.Opts}
}

func determineTimeUnit(t time.Time) xtime.Unit {
	ns := t.UnixNano()
	if ns%int64(time.Second) == 0 {
		return xtime.Second
	}
	if ns%int64(time.Millisecond) == 0 {
		return xtime.Millisecond
	}
	if ns%int64(time.Microsecond) == 0 {
		return xtime.Microsecond
	}
	return xtime.Nanosecond
}

func (ii *ingestIterator) Current() ingest.IterValue {
	if ii.pointIndex < len(ii.points) && ii.nextFieldIndex > 0 && len(ii.fields) > (ii.nextFieldIndex-1) {
		point := ii.points[ii.pointIndex]
		field := ii.fields[ii.nextFieldIndex-1]
		tags := copyTagsWithNewName(ii.tags, field.name)

		t := xtime.ToUnixNano(point.Time())

		value := ingest.IterValue{
			Tags:       tags,
			Datapoints: []ts.Datapoint{{Timestamp: t, Value: field.value}},
			Attributes: ts.DefaultSeriesAttributes(),
			Unit:       determineTimeUnit(point.Time()),
		}
		if ii.pointIndex < len(ii.metadatas) {
			value.Metadata = ii.metadatas[ii.pointIndex]
		}
		return value
	}
	return defaultValue
}

func (ii *ingestIterator) Reset() error {
	ii.pointIndex = 0
	ii.nextFieldIndex = 0
	ii.err = xerrors.NewMultiError()
	return nil
}

func (ii *ingestIterator) Error() error {
	return ii.err.FinalError()
}

func (ii *ingestIterator) SetCurrentMetadata(metadata ts.Metadata) {
	if len(ii.metadatas) == 0 {
		ii.metadatas = make([]ts.Metadata, len(ii.points))
	}
	if ii.pointIndex < len(ii.points) {
		ii.metadatas[ii.pointIndex] = metadata
	}
}

func (ii *ingestIterator) CurrentMetadata() ts.Metadata {
	if len(ii.metadatas) == 0 || ii.pointIndex >= len(ii.metadatas) {
		return ts.Metadata{}
	}
	return ii.metadatas[ii.pointIndex]
}

// NewInfluxWriterHandler returns a new influx write handler.
func NewInfluxWriterHandler(options options.HandlerOptions) http.Handler {
	return &ingestWriteHandler{handlerOpts: options,
		tagOpts:      options.TagOptions(),
		promRewriter: newPromRewriter()}
}

func (iwh *ingestWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Body == http.NoBody {
		xhttp.WriteError(w, xhttp.NewError(errors.New("empty request body"), http.StatusBadRequest))
		return
	}

	var bytes []byte
	var err error
	var reader io.ReadCloser

	if r.Header.Get("Content-Encoding") == "gzip" {
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			xhttp.WriteError(w, xhttp.NewError(err, http.StatusBadRequest))
			return
		}
	} else {
		reader = r.Body
	}

	bytes, err = ioutil.ReadAll(reader)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	err = reader.Close()
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	points, err := imodels.ParsePoints(bytes)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}
	opts := ingest.WriteOptions{}
	iter := &ingestIterator{points: points, tagOpts: iwh.tagOpts, promRewriter: iwh.promRewriter}
	batchErr := iwh.handlerOpts.DownsamplerAndWriter().WriteBatch(r.Context(), iter, opts)
	if batchErr == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var (
		errs              = batchErr.Errors()
		lastRegularErr    string
		lastBadRequestErr string
		numRegular        int
		numBadRequest     int
	)
	for _, err := range errs {
		switch {
		case client.IsBadRequestError(err):
			numBadRequest++
			lastBadRequestErr = err.Error()
		case xerrors.IsInvalidParams(err):
			numBadRequest++
			lastBadRequestErr = err.Error()
		default:
			numRegular++
			lastRegularErr = err.Error()
		}
	}

	var status int
	switch {
	case numBadRequest == len(errs):
		status = http.StatusBadRequest
	default:
		status = http.StatusInternalServerError
	}

	logger := logging.WithContext(r.Context(), iwh.handlerOpts.InstrumentOpts())
	logger.Error("write error",
		zap.String("remoteAddr", r.RemoteAddr),
		zap.Int("httpResponseStatusCode", status),
		zap.Int("numRegularErrors", numRegular),
		zap.Int("numBadRequestErrors", numBadRequest),
		zap.String("lastRegularError", lastRegularErr),
		zap.String("lastBadRequestErr", lastBadRequestErr))

	var resultErr string
	if lastRegularErr != "" {
		resultErr = fmt.Sprintf("retryable_errors: count=%d, last=%s",
			numRegular, lastRegularErr)
	}
	if lastBadRequestErr != "" {
		var sep string
		if lastRegularErr != "" {
			sep = ", "
		}
		resultErr = fmt.Sprintf("%s%sbad_request_errors: count=%d, last=%s",
			resultErr, sep, numBadRequest, lastBadRequestErr)
	}
	xhttp.WriteError(w, xhttp.NewError(errors.New(resultErr), status))
}
