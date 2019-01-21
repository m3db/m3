// Copyright (c) 2019 Uber Technologies, Inc.
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

package ingestcarbon

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/m3db/m3/src/metrics/carbon"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3x/instrument"
	m3xserver "github.com/m3db/m3x/server"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	carbonSeparatorByte  = byte('.')
	carbonSeparatorBytes = []byte{carbonSeparatorByte}

	errCannotGenerateTagsFromEmptyName = errors.New("cannot generate tags from empty name")
)

// StorageWriter is the interface that must be provided to the ingester so that it can
// write the ingested metrics.
type StorageWriter interface {
	Write(tags models.Tags, dp ts.Datapoint, unit xtime.Unit) error
}

// Options configures the ingester.
type Options struct {
	InstrumentOptions instrument.Options
	WorkerPool        xsync.PooledWorkerPool
}

// NewIngester returns an ingester for carbon metrics.
func NewIngester(
	storage StorageWriter,
	opts Options,
) m3xserver.Handler {
	return &ingester{
		storage: storage,
		opts:    opts,
	}
}

type ingester struct {
	storage StorageWriter
	opts    Options
	conn    net.Conn
}

func (i *ingester) Handle(conn net.Conn) {
	if i.conn != nil {
		// TODO: Something
	}
	i.conn = conn

	var (
		wg = sync.WaitGroup{}
		s  = carbon.NewScanner(conn)
	)

	for s.Scan() {
		_, timestamp, value := s.Metric()

		wg.Add(1)
		i.opts.WorkerPool.Go(func() {
			dp := ts.Datapoint{Timestamp: timestamp, Value: value}
			i.storage.Write(models.Tags{}, dp, xtime.Second)
			wg.Done()
		})
		// i.metrics.malformedCounter.Inc(int64(s.MalformedCount))
		s.MalformedCount = 0
	}

	// Wait for all outstanding writes
	wg.Wait()
}

func (i *ingester) Close() {
	// TODO: Log error
	i.conn.Close()
}

func newCarbonHandlerMetrics(m tally.Scope) carbonHandlerMetrics {
	writesScope := m.SubScope("writes")
	return carbonHandlerMetrics{
		unresolvedIDs:    writesScope.Counter("ids-policy-unresolved"),
		malformedCounter: writesScope.Counter("malformed"),
		readTimeLatency:  writesScope.Timer("read-time-latency"),
	}
}

type carbonHandlerMetrics struct {
	unresolvedIDs    tally.Counter
	malformedCounter tally.Counter
	readTimeLatency  tally.Timer
}

func generateTagsFromName(name []byte) (models.Tags, error) {
	if len(name) == 0 {
		return models.Tags{}, errCannotGenerateTagsFromEmptyName
	}
	var (
		numTags = bytes.Count(name, carbonSeparatorBytes) + 1
		tags    = make([]models.Tag, 0, numTags)
	)

	startIdx := 0
	tagNum := 0
	for i, charByte := range name {
		if charByte == carbonSeparatorByte {
			fmt.Println("appending: ", string(name[startIdx:i]))
			tags = append(tags, models.Tag{
				// TODO: Fix me
				Name:  []byte(fmt.Sprintf("__$%d__", tagNum)),
				Value: name[startIdx:i],
			})
			startIdx = i + 1
			tagNum++
		}
	}

	// Write out the final tag since the for loop above will miss anything
	// after the final separator. Note, that we make sure that the final
	// character in the name is not the separator because in that case there
	// would be no additional tag to add. I.E if the input was:
	//      foo.bar.baz
	// then the for loop would append foo and bar, but we would still need to
	// append baz, however, if the input was:
	//      foo.bar.baz.
	// then the foor loop would have appended foo, bar, and baz already.
	if name[len(name)-1] != carbonSeparatorByte {
		// TODO: Fix me
		fmt.Println("appending: ", string(name[startIdx:]))
		tags = append(tags, models.Tag{
			Name:  []byte(fmt.Sprintf("__$%d__", len(tags))),
			Value: name[startIdx:],
		})
	}

	return models.Tags{Tags: tags}, nil
}
