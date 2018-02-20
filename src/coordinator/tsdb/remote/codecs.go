package remote

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/m3coordinator"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"

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

// EncodeFetchMessage encodes fetch query and fetch options into rpc WriteMessage
func EncodeFetchMessage(query *storage.FetchQuery, queryID string) *rpc.FetchMessage {
	return &rpc.FetchMessage{
		Query:   encodeFetchQuery(query),
		Options: encodeFetchOptions(queryID),
	}

}

func encodeFetchQuery(query *storage.FetchQuery) *rpc.FetchQuery {
	return &rpc.FetchQuery{
		Start:       fromTime(query.Start),
		End:         fromTime(query.End),
		TagMatchers: encodeTagMatchers(query.TagMatchers),
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

// DecodeFetchMessage decodes rpc fetch message to read query and read options
func DecodeFetchMessage(message *rpc.FetchMessage) (*storage.FetchQuery, string, error) {
	query, err := decodeFetchQuery(message.GetQuery())
	if err != nil {
		return nil, "", err
	}
	return query, message.GetOptions().GetId(), nil
}

func decodeFetchQuery(query *rpc.FetchQuery) (*storage.FetchQuery, error) {
	tags, err := decodeTagMatchers(query.TagMatchers)
	if err != nil {
		return nil, err
	}

	return &storage.FetchQuery{
		TagMatchers: tags,
		Start:       toTime(query.Start),
		End:         toTime(query.End),
	}, nil
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

// EncodeWriteMessage encodes write query and write options into rpc WriteMessage
func EncodeWriteMessage(query *storage.WriteQuery, queryID string) *rpc.WriteMessage {
	return &rpc.WriteMessage{
		Query:   encodeWriteQuery(query),
		Options: encodeWriteOptions(queryID),
	}
}

func encodeWriteQuery(query *storage.WriteQuery) *rpc.WriteQuery {
	return &rpc.WriteQuery{
		Unit:       int32(query.Unit),
		Annotation: query.Annotation,
		Datapoints: encodeDatapoints(query.Datapoints),
		Tags:       query.Tags,
	}
}

// DecodeWriteMessage decodes rpc write message to write query and write options
func DecodeWriteMessage(message *rpc.WriteMessage) (*storage.WriteQuery, string) {
	return decodeWriteQuery(message.GetQuery()), message.GetOptions().GetId()
}

func decodeWriteQuery(query *rpc.WriteQuery) *storage.WriteQuery {
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
	}
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
