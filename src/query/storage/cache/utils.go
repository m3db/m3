package cache

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
)

type Timeseries []*prompb.TimeSeries

func (tss Timeseries) Len() int {
	return len(tss)
}

func (tss Timeseries) Swap(i, j int) {
	tss[i], tss[j] = tss[j], tss[i]
}

func (tss Timeseries) Less(i, j int) bool {
	return (&prompb.Labels{Labels: tss[i].Labels}).String() < (&prompb.Labels{Labels: tss[j].Labels}).String()
}

func createEmptyPromResult() *storage.PromResult {
	return &storage.PromResult{
		PromResult: &prompb.QueryResult{},
		Metadata:   block.NewResultMetadata(),
	}
}

// Given a fetch query, converts it into a key for Redis
// Give a prefix to differentiate between different types of keys (bucket vs. simple)
func KeyEncode(query *storage.FetchQuery, prefix string) string {
	res := make([]string, len(query.TagMatchers))
	for i, m := range query.TagMatchers {
		res[i] = m.String()
	}
	// Sort to guarantee we have the same order every time
	sort.Strings(res)

	label_key := strings.Join(res, ";")
	// Key becomes {labels}::{startTime}::{endTime}
	// For example, a key might look like
	// "fieldA=\"1\";fieldB=\"2\"::1659459470::1659459770"
	return fmt.Sprintf("%s::%s::%d::%d", prefix, label_key, query.Start.Unix(), query.End.Unix())
}

// Given a result, encodes it into string (or "" if error)
// Encodes an empty PromResult to {} to differentiate from ""
//
// We only encode the QueryResult portion of the PromResult struct
// We omit the metadata since we do not use it in computation
// and also this metadata also isn't accurate since we aren't getting it from M3DB
func resultEncode(result *storage.PromResult) (string, error) {
	if len(result.PromResult.Timeseries) == 0 {
		return EmptyResult, nil
	}
	// Custom Marshal function for QueryResults
	res, err := result.PromResult.Marshal()
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// Given a string, decodes it into a result (or nil if failed)
//
// Sets the metadata to a blank as we don't use it
// and we don't actually have metadata from M3DB for this result
func resultDecode(val []byte) (*storage.PromResult, error) {
	var result storage.PromResult
	result.PromResult = &prompb.QueryResult{}

	if len(val) == 0 {
		return nil, nil
	}

	// Check length first so we don't convert to string every time
	if len(val) == len([]byte(EmptyResult)) && string(val) == EmptyResult {
		result.Metadata = block.NewResultMetadata()
		return &result, nil
	}
	// Custom Unmarshal function for QueryResults
	if err := result.PromResult.Unmarshal(val); err != nil {
		return nil, err
	}
	result.Metadata = block.NewResultMetadata()
	return &result, nil
}

// Split the query into smaller queries per bucket based on bucket size
func splitQueryToBuckets(q *storage.FetchQuery, bucketSize time.Duration) []*storage.FetchQuery {
	var fetchQueries []*storage.FetchQuery

	start := q.Start.Truncate(bucketSize)
	for start.Before(q.End) {
		end := start.Add(bucketSize)
		if end.After(q.End) {
			end = q.End
		}
		fetchQueries = append(fetchQueries,
			&storage.FetchQuery{
				TagMatchers: q.TagMatchers,
				Start:       start,
				End:         end,
				Interval:    q.Interval,
			},
		)
		start = end
	}
	return fetchQueries
}

// Filter result in-place for all times after start (in s)
func filterResult(result *storage.PromResult, start int64) {
	if result != nil {
		for _, ts := range result.PromResult.Timeseries {
			idx := 0
			// Convert timestamp in millisecond to seconds
			for idx < len(ts.Samples) && ts.Samples[idx].Timestamp/1000 < start {
				idx++
			}
			ts.Samples = ts.Samples[idx:]
		}
	}
}

// Combines multiple M3DB results with the assumption that they are in order in time
func combineResult(results []*storage.PromResult) *storage.PromResult {
	result := &storage.PromResult{}
	result.Metadata = block.NewResultMetadata()
	result.PromResult = &prompb.QueryResult{}

	// Collect all the entries and combine them all at once after
	// This allows to make one alloc instead of the multiple that append would do
	// As per the link below, this saves on memory/CPU
	// https://stackoverflow.com/questions/37884361/concat-multiple-slices-in-golang
	ts_map := make(map[string][]*prompb.TimeSeries)
	for _, r := range results {
		for _, ts := range r.PromResult.Timeseries {
			key := (&prompb.Labels{Labels: ts.Labels}).String()
			if _, ok := ts_map[key]; !ok {
				ts_map[key] = make([]*prompb.TimeSeries, 0)
			}
			ts_map[key] = append(ts_map[key], ts)
		}
		result.Metadata.CombineMetadata(r.Metadata)
	}
	result.PromResult.Timeseries = make([]*prompb.TimeSeries, len(ts_map))
	i := 0
	for _, v := range ts_map {
		length := 0
		for _, sample := range v {
			length += len(sample.Samples)
		}
		samples := make([]prompb.Sample, length)

		cur_ts := 0
		idx := 0
		cnt := 0
		for cnt < length {
			// Handle case where we have an empty timeseries
			if len(v[cur_ts].Samples) == 0 {
				cur_ts++
				idx = 0
				continue
			}
			samples[cnt].Timestamp = v[cur_ts].Samples[idx].Timestamp
			samples[cnt].Value = float64(v[cur_ts].Samples[idx].Value)
			idx++
			cnt++

			if idx == len(v[cur_ts].Samples) {
				cur_ts++
				idx = 0
			}
		}
		result.PromResult.Timeseries[i] = &prompb.TimeSeries{
			Labels:  v[0].Labels,
			Samples: samples,
		}
		i++
	}
	return result
}
