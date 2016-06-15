package main

import (
	"fmt"
	"io"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/benchmark/fs"
	"code.uber.internal/infra/memtsdb/x/logging"
	xtime "code.uber.internal/infra/memtsdb/x/time"
)

type benchmark struct {
	logger logging.Logger

	startTime      time.Time
	windowSize     time.Duration
	inputTimeUnit  time.Duration
	encodeTimeUnit xtime.Unit
	inputReader    *fs.Reader

	encoder memtsdb.Encoder
	decoder memtsdb.Decoder

	numDatapoints   int64
	numEncodedBytes int64
	encodingTime    time.Duration
	decodingTime    time.Duration
}

func newBenchmark(
	logger logging.Logger,
	input string,
	startTime time.Time,
	windowSize time.Duration,
	inputTimeUnit time.Duration,
	encodeTimeUnit xtime.Unit,
	encoder memtsdb.Encoder,
	decoder memtsdb.Decoder,
) (*benchmark, error) {
	reader, err := fs.NewReader(input)
	if err != nil {
		return nil, fmt.Errorf("unable to create a new input reader: %v", err)
	}

	return &benchmark{
		logger:         logger,
		startTime:      startTime,
		windowSize:     windowSize,
		inputTimeUnit:  inputTimeUnit,
		encodeTimeUnit: encodeTimeUnit,
		inputReader:    reader,
		encoder:        encoder,
		decoder:        decoder,
	}, nil
}

func (th *benchmark) Run() {
	log := th.logger

	ns := xtime.ToNormalizedTime(th.startTime, th.inputTimeUnit)
	nw := xtime.ToNormalizedDuration(th.windowSize, th.inputTimeUnit)

	iter := th.inputReader.Iter()
	for iter.Next() {
		datapoints := iter.Value().Values
		if len(datapoints) == 0 {
			continue
		}
		th.encoder.Reset(th.startTime, 0)
		currentStart := ns
		currentEnd := currentStart + nw
		for i := 0; i < len(datapoints); i++ {
			if datapoints[i].Timestamp >= currentEnd {
				// start a new encoding block
				currentStart, currentEnd = th.rotate(datapoints[i].Timestamp, nw)
			}
			if err := th.encode(memtsdb.Datapoint{
				Timestamp: xtime.FromNormalizedTime(datapoints[i].Timestamp, th.inputTimeUnit),
				Value:     datapoints[i].Value,
			}); err != nil {
				log.Fatalf("error occurred when encoding datapoint: %v", err)
			}
		}
		th.decode()
	}

	if err := iter.Err(); err != nil {
		log.Fatalf("error occurred when iterating over input stream: %v", err)
	}
}

func (th *benchmark) encode(dp memtsdb.Datapoint) error {
	start := time.Now()
	if err := th.encoder.Encode(dp, th.encodeTimeUnit, nil); err != nil {
		return err
	}
	end := time.Now()
	th.encodingTime += end.Sub(start)
	th.numDatapoints++
	return nil
}

func getNumBytes(r io.Reader) int64 {
	if r == nil {
		return 0
	}
	numBytes := 0
	var b [1]byte
	for {
		n, err := r.Read(b[:])
		if err == io.EOF {
			break
		}
		numBytes += n
	}

	return int64(numBytes)
}

func (th *benchmark) decode() {
	stream := th.encoder.Stream()
	if stream == nil {
		return
	}
	th.numEncodedBytes += getNumBytes(stream)
	byteStream := th.encoder.Stream()

	start := time.Now()
	it := th.decoder.Decode(byteStream)
	// NB(xichen): consolidate these
	for it.Next() {
		it.Current()
	}
	end := time.Now()

	th.decodingTime += end.Sub(start)
}

func (th *benchmark) rotate(nt int64, nw int64) (int64, int64) {
	currentStart := nt - nt%nw
	currentEnd := currentStart + nw
	th.decode()
	th.encoder.Reset(xtime.FromNormalizedTime(currentStart, th.inputTimeUnit), 0)
	return currentStart, currentEnd
}

func (th *benchmark) Report() {
	log := th.logger

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
