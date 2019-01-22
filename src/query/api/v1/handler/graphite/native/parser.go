package native

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/util/json"
)

const (
	realTimeQueryThreshold   = time.Minute
	queryRangeShiftThreshold = 55 * time.Minute
	queryRangeShift          = 15 * time.Second
)

var (
	errNoTarget           = errors.NewInvalidParamsError(errors.New("no 'target' specified"))
	errFromNotBeforeUntil = errors.NewInvalidParamsError(errors.New("'from' must come before 'until'"))
)

// WriteRenderResponse writes the response to a render request
func WriteRenderResponse(
	w http.ResponseWriter,
	series ts.SeriesList,
) error {
	w.Header().Set("Content-Type", "application/json")
	return renderResultsJSON(w, series.Values)
}

const (
	tzOffsetForAbsoluteTime = time.Duration(0)
	maxTimeout              = time.Minute
	defaultTimeout          = time.Second * 5
	defaultMaxDatapoints    = 1024
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
func ParseRenderRequest(r *http.Request) (RenderRequest, error) {
	var (
		p   RenderRequest
		err error
		now = time.Now()
	)

	if err = r.ParseForm(); err != nil {
		return p, err
	}

	p.Targets = r.Form["target"]

	if len(p.Targets) == 0 {
		return p, errNoTarget
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
		return p, errors.NewInvalidParamsError(fmt.Errorf("invalid 'from': %s", fromString))
	}

	if p.Until, err = graphite.ParseTime(
		untilString,
		now,
		tzOffsetForAbsoluteTime,
	); err != nil {
		return p, errors.NewInvalidParamsError(fmt.Errorf("invalid 'until': %s", untilString))
	}

	if !p.From.Before(p.Until) {
		return p, errFromNotBeforeUntil
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
			return p, errors.NewRenamedError(err, fmt.Errorf("invalid 'offset': %s", err))
		}

		p.Until = p.Until.Add(dur)
		p.From = p.From.Add(dur)
	}

	maxDataPointsString := r.FormValue("maxDataPoints")
	if len(maxDataPointsString) != 0 {
		p.MaxDataPoints, err = strconv.ParseInt(maxDataPointsString, 10, 64)

		if err != nil || p.MaxDataPoints < 1 {
			return p, errors.NewInvalidParamsError(fmt.Errorf("invalid 'maxDataPoints': %s", maxDataPointsString))
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
		return p, errors.NewInvalidParamsError(fmt.Errorf("invalid 'compare': %s", compareString))
	} else if p.From.Before(compareFrom) {
		return p, errors.NewInvalidParamsError(fmt.Errorf("'compare' must be in the past"))
	} else {
		p.Compare = compareFrom.Sub(p.From)
	}

	timeout := r.FormValue("timeout")
	if timeout != "" {
		duration, err := time.ParseDuration(timeout)
		if err != nil {
			return p, errors.NewInvalidParamsError(fmt.Errorf("invalid 'timeout': %v", err))
		}
		if duration > maxTimeout {
			return p, errors.NewInvalidParamsError(fmt.Errorf("invalid 'timeout': greater than %v", maxTimeout))
		}
		p.Timeout = duration
	} else {
		p.Timeout = defaultTimeout
	}

	return p, nil
}

func renderResultsJSON(w io.Writer, series []*ts.Series) error {
	jw := json.NewWriter(w)
	jw.BeginArray()
	for _, s := range series {
		jw.BeginObject()
		jw.BeginObjectField("target")
		jw.WriteString(s.Name())
		jw.BeginObjectField("datapoints")
		jw.BeginArray()
		if s.AllNaN() {
			fmt.Println("All nan")
		}
		if !s.AllNaN() {
			fmt.Println("len", s.Len())
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
