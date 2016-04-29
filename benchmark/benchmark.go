package main

import (
	"bytes"
	"fmt"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/benchmark/fs"
	"code.uber.internal/infra/memtsdb/encoding"
	log "github.com/Sirupsen/logrus"
)

type benchmark struct {
	startTime   time.Time
	windowSize  time.Duration
	timeUnit    time.Duration
	inputReader *fs.Reader

	encoder encoding.Encoder
	decoder encoding.Decoder

	numDatapoints   int64
	numEncodedBytes int64
	encodingTime    time.Duration
	decodingTime    time.Duration
}

func newBenchmark(
	input string,
	startTime time.Time,
	windowSize, timeUnit time.Duration,
	encoder encoding.Encoder,
	decoder encoding.Decoder,
) (*benchmark, error) {
	reader, err := fs.NewReader(input)
	if err != nil {
		return nil, fmt.Errorf("unable to create a new input reader: %v", err)
	}

	return &benchmark{
		startTime:   startTime,
		windowSize:  windowSize,
		timeUnit:    timeUnit,
		inputReader: reader,
		encoder:     encoder,
		decoder:     decoder,
	}, nil
}

func (th *benchmark) Run() {
	ns := memtsdb.ToNormalizedTime(th.startTime, th.timeUnit)
	nw := memtsdb.ToNormalizedDuration(th.windowSize, th.timeUnit)

	iter := th.inputReader.Iter()
	for iter.Next() {
		datapoints := iter.Value().Values
		if len(datapoints) == 0 {
			continue
		}
		th.encoder.Reset(th.startTime)
		currentStart := ns
		currentEnd := currentStart + nw
		for i := 0; i < len(datapoints); i++ {
			if datapoints[i].Timestamp >= currentEnd {
				// start a new encoding block
				currentStart, currentEnd = th.rotate(datapoints[i].Timestamp, nw)
			}
			th.encode(encoding.Datapoint{
				Timestamp: memtsdb.FromNormalizedTime(datapoints[i].Timestamp, th.timeUnit),
				Value:     datapoints[i].Value,
			})
		}
		th.decode()
	}

	if err := iter.Err(); err != nil {
		log.Errorf("error occurred when iterating over input stream: %v", err)
	}
}

func (th *benchmark) encode(dp encoding.Datapoint) {
	start := time.Now()
	th.encoder.Encode(dp)
	end := time.Now()
	th.encodingTime += end.Sub(start)
	th.numDatapoints++
}

func (th *benchmark) decode() {
	encodedBytes := th.encoder.Bytes()
	if encodedBytes == nil {
		return
	}
	th.numEncodedBytes += int64(len(encodedBytes))
	byteStream := bytes.NewReader(encodedBytes)

	start := time.Now()
	it := th.decoder.Decode(byteStream)
	// NB(xichen): consolidate these
	for it.Next() {
		it.Value()
	}
	end := time.Now()

	th.decodingTime += end.Sub(start)
}

func (th *benchmark) rotate(nt int64, nw int64) (int64, int64) {
	currentStart := nt - nt%nw
	currentEnd := currentStart + nw
	th.decode()
	th.encoder.Reset(memtsdb.FromNormalizedTime(currentStart, th.timeUnit))
	return currentStart, currentEnd
}

func (th *benchmark) Report() {
	log.Infof(
		"Total datapoints encoded=%d, total number of encoded bytes=%d, total encoding time=%v, total decoding time=%v",
		th.numDatapoints,
		th.numEncodedBytes,
		th.encodingTime,
		th.decodingTime,
	)

	log.Infof(
		"Bytes per datapoint:%f, encoding time per datapoint:%v, decoding time per datapoint:%v",
		float64(th.numEncodedBytes)/float64(th.numDatapoints),
		time.Duration(int64(th.encodingTime)/int64(th.numDatapoints))*time.Nanosecond,
		time.Duration(int64(th.decodingTime)/int64(th.numDatapoints))*time.Nanosecond,
	)
}
