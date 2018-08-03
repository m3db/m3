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

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"

	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"go.uber.org/zap"
)

func calculateCardinality(fromFile string, logger *zap.Logger) (int, error) {
	lines, err := lineLength(fromFile)
	if err != nil {
		return 0, err
	}

	inFile, err := os.OpenFile(fromFile, os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer inFile.Close()

	scanner := bufio.NewScanner(inFile)

	tagsSeen := make(map[string]int)

	marker := lines / 10
	read := 0
	percent := 10

	for scanner.Scan() {
		tsdb := scanner.Text()
		ts, _ := marshalTSDBToProm(tsdb)
		tags := storage.PromLabelsToM3Tags(ts.GetLabels())
		id := tags.ID()
		tagsSeen[id]++

		read++
		if read%marker == 0 {
			logger.Info("read", zap.Int("percent", percent))
			percent += 10
		}
	}
	return len(tagsSeen), nil
}

func getFilePath(dataDir, dataFile string, worker, batchNumber int) string {
	return path.Join(dataDir, fmt.Sprintf("%s_%d_%d", dataFile, worker, batchNumber))
}

func ceilDivision(numerator, denominator int) int {
	return int(math.Ceil(float64(numerator) / float64(denominator)))
}

func convertToProm(fromFile, dir, toFile string, workers int, batchSize int, logger *zap.Logger) (int, error) {
	lines, err := lineLength(fromFile)
	logger.Info("Converting open_tsdb metrics to prom", zap.Int("lines", lines))

	if err != nil {
		return 0, err
	}

	// Breakup output files by worker, by batch size
	workerFiles := ceilDivision(lines, workers)
	batchFiles := ceilDivision(workerFiles, batchSize)
	fmt.Printf("\t%d files per worker\n\t%d batches per worker\n", workerFiles, batchFiles)

	inFile, err := os.OpenFile(fromFile, os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}

	err = os.Mkdir(dir, os.ModePerm)
	if err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(inFile)
	for w := 0; w < workers; w++ {
		for b := 0; b < batchFiles; b++ {
			outFilePath := getFilePath(dir, toFile, w, b)
			outFile, err := os.Create(outFilePath)
			if err != nil {
				return 0, err
			}
			series := make([]*prompb.TimeSeries, 0, batchSize)

			for line := 0; line < batchSize; line++ {
				if scanner.Scan() {
					tsdb := scanner.Text()
					prom, err := marshalTSDBToProm(tsdb)
					if err != nil {
						return 0, err
					}
					series = append(series, prom)
				} else {
					break
				}
			}
			if len(series) > 0 {
				outFile.Write(encodeWriteRequest(series))
			}
			err = outFile.Close()
			if err != nil {
				return 0, err
			}
		}
	}

	return lines, nil
}

func encodeWriteRequest(ts []*prompb.TimeSeries) []byte {
	req := &prompb.WriteRequest{
		Timeseries: ts,
	}
	data, _ := proto.Marshal(req)
	return snappy.Encode(nil, data)
}

// OpenTSDB style metrics
type metrics struct {
	Name  string            `json:"metric"`
	Time  int64             `json:"timestamp"`
	Tags  map[string]string `json:"tags"`
	Value float64           `json:"value"`
}

func marshalTSDBToProm(opentsdb string) (*prompb.TimeSeries, error) {
	var m metrics
	data := []byte(opentsdb)
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	labels := storage.TagsToPromLabels(m.Tags)
	samples := metricsPointsToSamples(m.Value, m.Time)
	return &prompb.TimeSeries{
		Labels:  labels,
		Samples: samples,
	}, nil
}

func metricsPointsToSamples(value float64, timestamp int64) []*prompb.Sample {
	return []*prompb.Sample{
		&prompb.Sample{
			Value:     value,
			Timestamp: timestamp,
		},
	}
}

func lineLength(fromFile string) (int, error) {
	file, err := os.OpenFile(fromFile, os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	lines := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines++
	}
	return lines, file.Close()
}
