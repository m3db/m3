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

package generate

import (
	"bytes"
	"math/rand"
	"sort"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding/testgen"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// Making SeriesBlock sortable
func (l SeriesBlock) Len() int      { return len(l) }
func (l SeriesBlock) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l SeriesBlock) Less(i, j int) bool {
	return bytes.Compare(l[i].ID.Bytes(), l[j].ID.Bytes()) < 0
}

// Block generates a SeriesBlock based on provided config
func Block(conf BlockConfig) SeriesBlock {
	if conf.NumPoints <= 0 {
		return nil
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	testData := make(SeriesBlock, len(conf.IDs))

	for i, name := range conf.IDs {
		datapoints := make([]TestValue, 0, conf.NumPoints)
		for j := 0; j < conf.NumPoints; j++ {
			timestamp := conf.Start.Add(time.Duration(j) * time.Second)
			if conf.AnnGen == nil {
				datapoints = append(datapoints, TestValue{
					Datapoint: ts.Datapoint{
						TimestampNanos: timestamp,
						Value:          testgen.GenerateFloatVal(r, 3, 1),
					},
				})
			} else {
				datapoints = append(datapoints, TestValue{
					Datapoint: ts.Datapoint{
						TimestampNanos: timestamp,
						Value:          0,
					},
					Annotation: conf.AnnGen.Next(),
				})
			}
		}
		testData[i] = Series{
			ID:   ident.StringID(name),
			Tags: conf.Tags,
			Data: datapoints,
		}
	}
	return testData
}

// BlocksByStart generates a map of SeriesBlocks keyed by Start time
// for the provided configs
func BlocksByStart(confs []BlockConfig) SeriesBlocksByStart {
	seriesMaps := make(map[xtime.UnixNano]SeriesBlock)
	for _, conf := range confs {
		key := conf.Start
		seriesMaps[key] = append(seriesMaps[key], Block(conf)...)
	}
	return seriesMaps
}

// ToPointsByTime converts a SeriesBlocksByStart to SeriesDataPointsByTime
func ToPointsByTime(seriesMaps SeriesBlocksByStart) SeriesDataPointsByTime {
	var pointsByTime SeriesDataPointsByTime
	for _, blks := range seriesMaps {
		for _, blk := range blks {
			for _, dp := range blk.Data {
				pointsByTime = append(pointsByTime, SeriesDataPoint{
					ID:    blk.ID,
					Value: dp,
				})
			}
		}
	}
	sort.Sort(pointsByTime)
	return pointsByTime
}

// Dearrange de-arranges the list by the defined percent.
func (l SeriesDataPointsByTime) Dearrange(percent float64) SeriesDataPointsByTime {
	numDis := percent * float64(len(l))
	disEvery := int(float64(len(l)) / numDis)

	newArr := make(SeriesDataPointsByTime, 0, len(l))
	for i := 0; i < len(l); i += disEvery {
		ti := i + disEvery
		if ti >= len(l) {
			newArr = append(newArr, l[i:]...)
			break
		}

		newArr = append(newArr, l[ti])
		newArr = append(newArr, l[i:ti]...)
	}

	return newArr
}

// Making SeriesDataPointsByTimes sortable

func (l SeriesDataPointsByTime) Len() int      { return len(l) }
func (l SeriesDataPointsByTime) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l SeriesDataPointsByTime) Less(i, j int) bool {
	if l[i].Value.TimestampNanos != l[j].Value.TimestampNanos {
		return l[i].Value.TimestampNanos < l[j].Value.TimestampNanos
	}
	return bytes.Compare(l[i].ID.Bytes(), l[j].ID.Bytes()) < 0
}
