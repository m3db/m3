package remote

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"

	"github.com/m3db/m3coordinator/models"
	xtime "github.com/m3db/m3x/time"
)

func fromTime(t time.Time) int64 {
	return storage.TimeToTimestamp(t)
}

func toTime(t int64) time.Time {
	return storage.TimestampToTime(t)
}

// EncodeFetchResult encodes fetch result to rpc result
func EncodeFetchResult(sResult *storage.FetchResult) *rpc.FetchResult {
	series := make([]*rpc.Series, len(sResult.SeriesList))
	for i, result := range sResult.SeriesList {
		vLen := result.Len()
		vals := make([]float32, vLen)
		for j := 0; j < vLen; j++ {
			vals[j] = float32(result.ValueAt(j))
		}
		series[i] = &rpc.Series{
			Name:          result.Name(),
			Values:        vals,
			StartTime:     fromTime(result.StartTime()),
			Tags:          result.Tags,
			Specification: result.Specification,
			MillisPerStep: int32(result.MillisPerStep()),
		}
	}
	return &rpc.FetchResult{Series: series}
}

// DecodeFetchResult decodes fetch results from a GRPC-compatible type.
func DecodeFetchResult(ctx context.Context, rpcSeries []*rpc.Series) []*ts.Series {
	tsSeries := make([]*ts.Series, len(rpcSeries))
	for i, series := range rpcSeries {
		tsSeries[i] = decodeTs(ctx, series)
	}
	return tsSeries
}

func decodeTs(ctx context.Context, r *rpc.Series) *ts.Series {
	millis, rValues := int(r.GetMillisPerStep()), r.GetValues()
	values := ts.NewValues(ctx, millis, len(rValues))

	for i, v := range rValues {
		values.SetValueAt(i, float64(v))
	}

	start, tags := toTime(r.GetStartTime()), models.Tags(r.GetTags())

	series := ts.NewSeries(ctx, r.GetName(), start, values, tags)
	series.Specification = r.GetSpecification()
	return series
}

// EncodeReadQuery encodes read query to rpc fetch query
func EncodeReadQuery(query *storage.ReadQuery, queryID string) *rpc.FetchQuery {
	return &rpc.FetchQuery{
		Start:       fromTime(query.Start),
		End:         fromTime(query.End),
		TagMatchers: encodeTagMatchers(query.TagMatchers),
		Options:     encodeFetchOptions(queryID),
	}
}

func encodeTagMatchers(modelMatchers models.Matchers) []*rpc.Matcher {
	matchers := make([]*rpc.Matcher, len(modelMatchers))
	for i, matcher := range modelMatchers {
		matchers[i] = &rpc.Matcher{
			Name:  matcher.Name,
			Value: matcher.Value,
			Type:  int64(matcher.Type),
		}
	}

	return matchers
}

func encodeFetchOptions(queryID string) *rpc.FetchOptions {
	return &rpc.FetchOptions{
		Id: queryID,
	}
}

// DecodeFetchQuery decodes rpc fetch query to read query
func DecodeFetchQuery(query *rpc.FetchQuery) (*storage.ReadQuery, string, error) {
	tags, err := decodeTagMatchers(query.TagMatchers)
	if err != nil {
		return nil, "", err
	}

	return &storage.ReadQuery{
		TagMatchers: tags,
		Start:       toTime(query.Start),
		End:         toTime(query.End),
	}, query.GetOptions().GetId(), nil
}

func decodeTagMatchers(rpcMatchers []*rpc.Matcher) (models.Matchers, error) {
	matchers := make([]*models.Matcher, len(rpcMatchers))
	for i, matcher := range rpcMatchers {
		matchType, name, value := models.MatchType(matcher.GetType()), matcher.GetName(), matcher.GetValue()
		mMatcher, err := models.NewMatcher(matchType, name, value)
		if err != nil {
			return matchers, err
		}
		matchers[i] = mMatcher
	}
	return models.Matchers(matchers), nil
}

// EncodeWriteQuery encodes write query to rpc write query
func EncodeWriteQuery(query *storage.WriteQuery, queryID string) *rpc.WriteQuery {
	return &rpc.WriteQuery{
		Unit:       int32(query.Unit),
		Annotation: query.Annotation,
		Datapoints: encodeDatapoints(query.Datapoints),
		Tags:       query.Tags,
		Options:    encodeWriteOptions(queryID),
	}
}

// DecodeWriteQuery decodes rpc write query to write query
func DecodeWriteQuery(query *rpc.WriteQuery) (*storage.WriteQuery, string) {
	points := make([]*ts.Datapoint, len(query.GetDatapoints()))
	for i, point := range query.GetDatapoints() {
		points[i] = &ts.Datapoint{
			Timestamp: toTime(point.GetTimestamp()),
			Value:     float64(point.GetValue()),
		}
	}
	return &storage.WriteQuery{
		Tags:       query.GetTags(),
		Datapoints: ts.Datapoints(points),
		Unit:       xtime.Unit(query.GetUnit()),
		Annotation: query.Annotation,
	}, query.GetOptions().GetId()
}

func encodeDatapoints(tsPoints ts.Datapoints) []*rpc.Datapoint {
	datapoints := make([]*rpc.Datapoint, len(tsPoints))
	for i, point := range tsPoints {
		datapoints[i] = &rpc.Datapoint{
			Timestamp: fromTime(point.Timestamp),
			Value:     float32(point.Value),
		}
	}
	return datapoints
}

func encodeWriteOptions(queryID string) *rpc.WriteOptions {
	return &rpc.WriteOptions{
		Id: queryID,
	}
}
