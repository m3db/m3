package cache

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
	"go.uber.org/zap"
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

// Print labels (debug purposes)
func printLabels(a, b []prompb.Label, logger *zap.Logger) {
	arr1 := make([]string, 0)
	for i := range a {
		arr1 = append(arr1, a[i].String())
	}
	arr2 := make([]string, 0)
	for i := range b {
		arr2 = append(arr2, b[i].String())
	}
	logger.Info("label differences", zap.Strings("actual", arr1), zap.Strings("expected", arr2))
}

// Check if lists of labels are equal
func labelsEqual(a, b []prompb.Label, logger *zap.Logger) bool {
	if len(a) != len(b) {
		printLabels(a, b, logger)
		return false
	}
	for i := range a {
		val := (string(a[i].Name) == string(b[i].Name) &&
			string(a[i].Value) == string(b[i].Value))
		if !val {
			printLabels(a, b, logger)
			return false
		}
	}
	return true
}

// Print samples (debug purposes)
func printSamples(a, b []prompb.Sample, logger *zap.Logger) {
	arr1 := make([]string, 0)
	for i := range a {
		arr1 = append(arr1, a[i].String())
	}
	arr2 := make([]string, 0)
	for i := range b {
		arr2 = append(arr2, b[i].String())
	}
	logger.Info("sample differences", zap.Strings("actual", arr1), zap.Strings("expected", arr2))
}

// Check if lists of samples are equal. Only returns false if they don't match before time {end} (in ms)
// If {end} is 0, then there is no upper bound on timestamp checking
func samplesEqual(a, b []prompb.Sample, end int64, logger *zap.Logger) bool {
	// Can't check lengths due to potential variance of M3DB at end of query range (i.e. current time)
	for i := range a {
		if i >= len(b) {
			printSamples(a, b, logger)
			return false
		}
		val := (a[i].Timestamp == b[i].Timestamp &&
			a[i].Value == b[i].Value)
		if !val {
			// Account for NaN case
			if math.IsNaN(a[i].Value) && math.IsNaN(b[i].Value) {
				continue
			}
			// If we checked all timestamps so we are after provided timestamp, then don't return false
			// If end == 0 was specified, then we don't make this exception
			if end != 0 && a[i].Timestamp >= end && b[i].Timestamp >= end {
				printSamples(a, b, logger)
				return true
			}
			return false
		}
	}
	return true
}

// Check if lists of timeseries are equal (see samplesEqual for usage of {end} param)
func TimeseriesEqual(a, b []*prompb.TimeSeries, end int64, logger *zap.Logger) bool {
	if len(a) != len(b) {
		logger.Info("Timeseries lengths unequal", zap.Int("actual length", len(a)), zap.Int("expected length", len(b)))
		return false
	}
	// Sort to ensure we are matching up the timeseries to each other properly
	sort.Slice(a, func(i, j int) bool {
		return (&prompb.Labels{Labels: a[i].Labels}).String() < (&prompb.Labels{Labels: a[j].Labels}).String()
	})
	sort.Slice(b, func(i, j int) bool {
		return (&prompb.Labels{Labels: b[i].Labels}).String() < (&prompb.Labels{Labels: b[j].Labels}).String()
	})
	for i := range a {
		e1 := labelsEqual(a[i].Labels, b[i].Labels, logger)
		e2 := samplesEqual(a[i].Samples, b[i].Samples, end, logger)
		// If one doesn't equal
		if !e1 || !e2 {
			return false
		}
	}
	return true
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
	nonempty := make([]*prompb.TimeSeries, 0)
	if result != nil {
		for _, ts := range result.PromResult.Timeseries {
			idx := 0
			for idx < len(ts.Samples) && ts.Samples[idx].Timestamp < start {
				idx++
			}
			if idx != len(ts.Samples) {
				nonempty = append(nonempty, &prompb.TimeSeries{
					Labels:  ts.Labels,
					Samples: ts.Samples[idx:],
				})
			}
		}
	}
	result.PromResult.Timeseries = nonempty
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
		if length == 0 {
			continue
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
