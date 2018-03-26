package common

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/prompb"
	"github.com/m3db/m3coordinator/storage"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
)

const (
	// MetricsLen is used to create the objects that store the parsed metrics
	MetricsLen = 100000
)

// Metrics is the OpenTSDB style metrics
type Metrics struct {
	Name  string            `json:"metric"`
	Time  int64             `json:"timestamp"`
	Tags  map[string]string `json:"tags"`
	Value float64           `json:"value"`
}

// M3Metric is a lighterweight Metrics struct
type M3Metric struct {
	ID    string
	Time  time.Time
	Value float64
}

// ConvertToM3 parses the json file that is generated from InfluxDB's bulk_data_gen tool
func ConvertToM3(fileName string, workers int, f func(*M3Metric)) {
	metricChannel := make(chan *M3Metric, MetricsLen)
	dataChannel := make(chan []byte, MetricsLen)
	wg := new(sync.WaitGroup)
	workFunction := func() {
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				unmarshalMetrics(dataChannel, metricChannel)
				wg.Done()
			}()
		}
		go func() {
			for metric := range metricChannel {
				f(metric)
			}
		}()
		wg.Wait()
		close(metricChannel)
	}

	convertToGeneric(fileName, dataChannel, workFunction)
}

// ConvertToProm parses the json file that is generated from InfluxDB's bulk_data_gen tool into Prom format
func ConvertToProm(fileName string, workers int, batchSize int, f func(*bytes.Reader)) {
	metricChannel := make(chan *bytes.Reader, MetricsLen)
	dataChannel := make(chan []byte, MetricsLen)
	wg := new(sync.WaitGroup)
	workFunction := func() {
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				marshalTSDBToProm(dataChannel, metricChannel, batchSize)
				wg.Done()
			}()
		}
		go func() {
			for metric := range metricChannel {
				f(metric)
			}
		}()
		wg.Wait()
		close(metricChannel)
	}
	convertToGeneric(fileName, dataChannel, workFunction)
}

func convertToGeneric(fileName string, dataChannel chan<- []byte, workFunction func()) {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		log.Fatalf("unable to read json file, got error: %v\n", err)
	}

	defer fd.Close()

	scanner := bufio.NewScanner(fd)

	go func() {
		for scanner.Scan() {
			data := bytes.TrimSpace(scanner.Bytes())
			b := make([]byte, len(data))
			copy(b, data)
			dataChannel <- b
		}
		close(dataChannel)
	}()
	workFunction()

	if err := scanner.Err(); err != nil {
		log.Fatalf("scanner encountered error: %v\n", err)
	}
}

func unmarshalMetrics(dataChannel <-chan []byte, metricChannel chan<- *M3Metric) {
	for data := range dataChannel {
		if len(data) == 0 {
			continue
		}
		var m Metrics
		if err := json.Unmarshal(data, &m); err != nil {
			log.Fatalf("failed to unmarshal metrics, got error: %v\n", err)
		}

		metricChannel <- &M3Metric{ID: id(m.Tags, m.Name), Time: storage.TimestampToTime(m.Time), Value: m.Value}
	}
}

func id(lowerCaseTags map[string]string, name string) string {
	sortedKeys := make([]string, len(lowerCaseTags))
	var buffer = bytes.NewBuffer(nil)
	buffer.WriteString(strings.ToLower(name))

	// Generate tags in alphabetical order & write to buffer
	i := 0
	for key := range lowerCaseTags {
		sortedKeys = append(sortedKeys, key)
		i++
	}
	sort.Strings(sortedKeys)

	for i = 0; i < len(sortedKeys)-1; i++ {
		buffer.WriteString(sortedKeys[i])
		buffer.WriteString(lowerCaseTags[sortedKeys[i]])
	}

	return buffer.String()
}

func metricsToPromTS(m Metrics) *prompb.TimeSeries {
	labels := storage.TagsToPromLabels(m.Tags)
	samples := metricsPointsToSamples(m.Value, m.Time)
	return &prompb.TimeSeries{
		Labels:  labels,
		Samples: samples,
	}
}

func marshalTSDBToProm(dataChannel <-chan []byte, metricChannel chan<- *bytes.Reader, batchSize int) {
	timeseries := make([]*prompb.TimeSeries, batchSize)
	idx := 0
	for data := range dataChannel {
		if len(data) == 0 {
			continue
		}
		var m Metrics
		if err := json.Unmarshal(data, &m); err != nil {
			log.Fatalf("failed to unmarshal metrics for prom conversion, got error: %v\n", err)
		}
		timeseries[idx] = metricsToPromTS(m)
		idx++
		if idx == batchSize {
			metricChannel <- encodeWriteRequest(timeseries)
			idx = 0
		}
	}
	if idx > 0 {
		// Send the remaining series
		metricChannel <- encodeWriteRequest(timeseries[:idx])
	}
}

func encodeWriteRequest(ts []*prompb.TimeSeries) *bytes.Reader {
	req := &prompb.WriteRequest{
		Timeseries: ts,
	}
	data, _ := proto.Marshal(req)
	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)
	return b
}

func metricsPointsToSamples(value float64, timestamp int64) []*prompb.Sample {
	return []*prompb.Sample{
		&prompb.Sample{
			Value:     value,
			Timestamp: timestamp,
		},
	}
}
