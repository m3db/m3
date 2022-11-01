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

package graphite

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/graphite/pickle"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/json"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	realTimeQueryThreshold   = time.Minute
	queryRangeShiftThreshold = 55 * time.Minute
	queryRangeShift          = 15 * time.Second
	pickleFormat             = "pickle"
)

var (
	errNoTarget           = errors.NewInvalidParamsError(errors.New("no 'target' specified"))
	errFromNotBeforeUntil = errors.NewInvalidParamsError(errors.New("'from' must come before 'until'"))
)

// WriteRenderResponse writes the response to a render request
func WriteRenderResponse(
	w http.ResponseWriter,
	series ts.SeriesList,
	format string,
	opts renderResultsJSONOptions,
) error {
	if format == pickleFormat {
		w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeOctetStream)
		return renderResultsPickle(w, series.Values)
	}

	// NB: return json unless requesting specifically `pickleFormat`
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)
	return renderResultsJSON(w, series.Values, opts)
}

const (
	tzOffsetForAbsoluteTime = time.Duration(0)
	maxTimeout              = time.Minute
)

// RenderRequest are the arguments to a render call.
type RenderRequest struct {
	Targets       []string
	Format        string
	From          time.Time
	Until         time.Time
	MaxDataPoints int64
	Compare       time.Duration
	Timeout       time.Duration
}

// ParseRenderRequest parses the arguments to a render call from an incoming request.
func ParseRenderRequest(
	r *http.Request,
	opts options.HandlerOptions,
) (RenderRequest, *storage.FetchOptions, error) {
	fetchOpts, err := opts.GraphiteRenderFetchOptionsBuilder().NewFetchOptions(r)
	if err != nil {
		return RenderRequest{}, nil, err
	}

	if err := r.ParseForm(); err != nil {
		return RenderRequest{}, nil, err
	}

	var (
		p = RenderRequest{
			Timeout: fetchOpts.Timeout,
		}
		now = time.Now()
	)
	p.Targets = r.Form["target"]
	if len(p.Targets) == 0 {
		return p, nil, errNoTarget
	}

	fromString, untilString := r.FormValue("from"), r.FormValue("until")
	if len(fromString) == 0 {
		fromString = "-30min"
	}

	if len(untilString) == 0 {
		untilString = "now"
	}

	if p.From, err = graphite.ParseTime(
		fromString,
		now,
		tzOffsetForAbsoluteTime,
	); err != nil {
		return p, nil, errors.NewInvalidParamsError(fmt.Errorf("invalid 'from': %s", fromString))
	}

	if p.Until, err = graphite.ParseTime(
		untilString,
		now,
		tzOffsetForAbsoluteTime,
	); err != nil {
		return p, nil, errors.NewInvalidParamsError(fmt.Errorf("invalid 'until': %s", untilString))
	}

	if !p.From.Before(p.Until) {
		return p, nil, errFromNotBeforeUntil
	}

	// If this is a real-time query, and the query range is large enough, we shift the query
	// range slightly to take into account the clock skew between the client's local time and
	// the server's local time in order to take advantage of possibly higher-resolution data.
	// In the future we could potentially distinguish absolute time and relative time and only
	// use the time range for policy resolution, although we need to be careful when passing
	// the range for cross-DC queries.
	isRealTimeQuery := now.Sub(p.Until) < realTimeQueryThreshold
	isLargeRangeQuery := p.Until.Sub(p.From) > queryRangeShiftThreshold
	if isRealTimeQuery && isLargeRangeQuery {
		p.From = p.From.Add(queryRangeShift)
		p.Until = p.Until.Add(queryRangeShift)
	}

	offset := r.FormValue("offset")
	if len(offset) > 0 {
		dur, err := graphite.ParseDuration(offset)
		if err != nil {
			err = errors.NewInvalidParamsError(err)
			return p, nil, errors.NewRenamedError(err, fmt.Errorf("invalid 'offset': %s", err))
		}

		p.Until = p.Until.Add(dur)
		p.From = p.From.Add(dur)
	}

	maxDataPointsString := r.FormValue("maxDataPoints")
	if len(maxDataPointsString) != 0 {
		p.MaxDataPoints, err = strconv.ParseInt(maxDataPointsString, 10, 64)

		if err != nil || p.MaxDataPoints < 1 {
			return p, nil, errors.NewInvalidParamsError(fmt.Errorf("invalid 'maxDataPoints': %s", maxDataPointsString))
		}
	} else {
		p.MaxDataPoints = math.MaxInt64
	}

	compareString := r.FormValue("compare")

	if compareFrom, err := graphite.ParseTime(
		compareString,
		p.From,
		tzOffsetForAbsoluteTime,
	); err != nil && len(compareString) != 0 {
		return p, nil, errors.NewInvalidParamsError(fmt.Errorf("invalid 'compare': %s", compareString))
	} else if p.From.Before(compareFrom) {
		return p, nil, errors.NewInvalidParamsError(fmt.Errorf("'compare' must be in the past"))
	} else {
		p.Compare = compareFrom.Sub(p.From)
	}

	return p, fetchOpts, nil
}

type renderResultsJSONOptions struct {
	renderSeriesAllNaNs bool
}

func renderResultsJSON(
	w io.Writer,
	series []*ts.Series,
	opts renderResultsJSONOptions,
) error {
	jw := json.NewWriter(w)
	jw.BeginArray()
	for _, s := range series {
		jw.BeginObject()
		jw.BeginObjectField("target")
		jw.WriteString(s.Name())
		jw.BeginObjectField("datapoints")
		jw.BeginArray()

		if !s.AllNaN() || opts.renderSeriesAllNaNs {
			for i := 0; i < s.Len(); i++ {
				timestamp, val := s.StartTimeForStep(i), s.ValueAt(i)
				jw.BeginArray()
				jw.WriteFloat64(val)
				jw.WriteInt(int(timestamp.Unix()))
				jw.EndArray()
			}
		}

		jw.EndArray()
		jw.BeginObjectField("step_size_ms")
		jw.WriteInt(s.MillisPerStep())

		jw.EndObject()
	}
	jw.EndArray()
	return jw.Close()
}

func renderResultsPickle(w io.Writer, series []*ts.Series) error {
	pw := pickle.NewWriter(w)
	pw.BeginList()

	for _, s := range series {
		pw.BeginDict()
		pw.WriteDictKey("name")
		pw.WriteString(s.Name())

		pw.WriteDictKey("start")
		pw.WriteInt(int(s.StartTime().UTC().Unix()))

		pw.WriteDictKey("end")
		pw.WriteInt(int(s.EndTime().UTC().Unix()))

		pw.WriteDictKey("step")
		pw.WriteInt(s.MillisPerStep() / 1000)

		pw.WriteDictKey("values")
		pw.BeginList()
		for i := 0; i < s.Len(); i++ {
			pw.WriteFloat64(s.ValueAt(i))
		}
		pw.EndList()

		pw.EndDict()
	}

	pw.EndList()

	return pw.Close()
}
