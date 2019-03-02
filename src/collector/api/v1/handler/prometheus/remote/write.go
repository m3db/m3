package remote

import (
	"fmt"
	"io/ioutil"
	"math"
	"net/http"

	"github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/instrument"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

const (
	WriteURL         = "/api/v1/write"
	ReportHTTPMethod = http.MethodPost
)

type PromWriteHandler struct {
	reporter       reporter.Reporter
	encoderPool    serialize.TagEncoderPool
	decoderPool    serialize.TagDecoderPool
	instrumentOpts instrument.Options
}

func NewPromWriteHandler(
	reporter reporter.Reporter,
	encoderPool serialize.TagEncoderPool,
	decoderPool serialize.TagDecoderPool,
	instrumentOpts instrument.Options,
) *PromWriteHandler {
	return &PromWriteHandler{
		reporter:       reporter,
		encoderPool:    encoderPool,
		decoderPool:    decoderPool,
		instrumentOpts: instrumentOpts,
	}
}

func (h *PromWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	err = proto.Unmarshal(data, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, v := range req.Timeseries {
		id, err := h.newMetricID(v.Labels)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, s := range v.Samples {
			if math.IsNaN(s.Value) {
				continue
			}
			err = h.reporter.ReportGauge(id, s.Value)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (h *PromWriteHandler) newMetricID(l []*prompb.Label) (id.ID, error) {
	tags := models.NewTags(len(l), models.NewTagOptions())
	for _, v := range l {
		tags = tags.AddTag(models.Tag{Name: []byte(v.Name), Value: []byte(v.Value)})
	}
	tagsIter := storage.TagsToIdentTagIterator(tags)

	encoder := h.encoderPool.Get()
	defer encoder.Finalize()

	if err := encoder.Encode(tagsIter); err != nil {
		return nil, err
	}

	data, ok := encoder.Data()
	if !ok {
		return nil, fmt.Errorf("failed to encode")
	}

	// Take a copy of the pooled encoder's bytes
	bytes := append([]byte(nil), data.Bytes()...)

	metricTagsIter := serialize.NewMetricTagsIterator(h.decoderPool.Get(), nil)
	metricTagsIter.Reset(bytes)
	return metricTagsIter, nil
}
