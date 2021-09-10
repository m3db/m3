package promremotewrite

import (
	"time"

	"github.com/golang/snappy"
	"github.com/m3db/m3/src/query/storage"
	"github.com/prometheus/prometheus/prompb"
)

func encodeWriteQuery(query *storage.WriteQuery) ([]byte, error){
	promQuery := mapWriteQuery(query)
	data, err := promQuery.Marshal()
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, data), nil
}


func mapWriteQuery(query *storage.WriteQuery) *prompb.WriteRequest {
	labels := make([]prompb.Label, len(query.Tags().Tags))
	samples := make([]prompb.Sample, len(query.Datapoints()))

	for i, tag := range query.Tags().Tags {
		labels[i] = prompb.Label{
			Name: string(tag.Name),
			Value: string(tag.Value),
		}
	}

	for i, dp := range query.Datapoints() {
		samples[i] = prompb.Sample{
			Value: dp.Value,
			Timestamp: dp.Timestamp.ToNormalizedTime(time.Millisecond),
		}
	}

	return &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: labels,
				Samples: samples,
			},
		},
	}
}
