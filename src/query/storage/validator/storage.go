// Copyright (c) 2018 Uber Technologies, Inc.
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

package validator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
)

type debugStorage struct {
	seriesList       []*ts.Series
	lookbackDuration time.Duration
}

// NewStorage creates a new debug storage instance.
func NewStorage(promReadResp prometheus.PromResp, lookbackDuration time.Duration) (storage.Storage, error) {
	seriesList, err := PromResultToSeriesList(promReadResp, models.NewTagOptions())
	if err != nil {
		return nil, err
	}

	return &debugStorage{
		seriesList:       seriesList,
		lookbackDuration: lookbackDuration,
	}, nil
}

func (s *debugStorage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.FetchResult, error) {
	return &storage.FetchResult{
		SeriesList: s.seriesList,
	}, nil
}

func (s *debugStorage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	fetchResult, err := s.Fetch(ctx, query, options)
	if err != nil {
		return block.Result{}, err
	}

	return storage.FetchResultToBlockResult(fetchResult, query, s.lookbackDuration, options.Enforcer)
}

// PromResultToSeriesList converts a prom result to a series list
func PromResultToSeriesList(promReadResp prometheus.PromResp, tagOptions models.TagOptions) ([]*ts.Series, error) {
	if promReadResp.Data.ResultType != "matrix" {
		return nil, fmt.Errorf("unsupported result type found: %s", promReadResp.Data.ResultType)
	}

	results := promReadResp.Data.Result

	seriesList := make([]*ts.Series, len(results))

	for i, result := range results {
		dps := make(ts.Datapoints, len(result.Values))
		for i, dp := range result.Values {
			if len(dp) != 2 {
				return nil, errors.New("misformatted datapoint found")
			}

			dpTimeFloat, ok := dp[0].(float64)
			if !ok {
				return nil, fmt.Errorf("unable to cast to float: %v", dp[0])
			}
			dps[i].Timestamp = time.Unix(0, int64(dpTimeFloat)*int64(time.Second))

			dpValStr, ok := dp[1].(string)
			if !ok {
				return nil, fmt.Errorf("unable to cast to string: %v", dp[1])
			}

			if dpValStr == "NaN" {
				dps[i].Value = math.NaN()
			} else {
				s, err := strconv.ParseFloat(dpValStr, 64)
				if err != nil {
					return nil, err
				}

				dps[i].Value = s
			}
		}

		metricName := string(tagOptions.MetricName())
		bucketName := string(tagOptions.BucketName())
		tags := models.NewTags(len(result.Metric), tagOptions)
		for name, val := range result.Metric {
			if name == metricName {
				tags = tags.SetName([]byte(val))
			} else if name == bucketName {
				tags = tags.SetBucket([]byte(val))
			} else {
				tags = tags.AddTag(models.Tag{
					Name:  []byte(name),
					Value: []byte(val),
				})
			}
		}

		// NB: if there is no tag for series name here, the input is a Prometheus
		// query result for a function that mutates the output tags and drops `name`
		// which is a valid case.
		//
		// It's safe to set ts.Series.Name() here to a default value, as this field
		// is used as a minor optimization for presenting grafana output, and as
		// such, series names are not validated for equality.
		name, exists := tags.Name()
		if !exists {
			name = []byte("default")
		}

		seriesList[i] = ts.NewSeries(
			name,
			dps,
			tags,
		)
	}

	return seriesList, nil
}

func (s *debugStorage) Type() storage.Type {
	return storage.TypeDebug
}

func (s *debugStorage) SearchSeries(
	ctx context.Context,
	query *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, errors.New("SearchSeries not implemented")
}

func (s *debugStorage) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	return nil, errors.New("CompleteTags not implemented")
}

func (s *debugStorage) Close() error {
	return nil
}

func (s *debugStorage) Write(
	ctx context.Context,
	query *storage.WriteQuery,
) error {
	return errors.New("write not implemented")
}
