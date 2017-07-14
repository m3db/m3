package generate

import (
	"bytes"
	"math/rand"
	"sort"
	"time"

	"github.com/m3db/m3db/encoding/testgen"
	"github.com/m3db/m3db/ts"
)

// Making SeriesBlock sortable
func (l SeriesBlock) Len() int      { return len(l) }
func (l SeriesBlock) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l SeriesBlock) Less(i, j int) bool {
	return bytes.Compare(l[i].ID.Data().Get(), l[j].ID.Data().Get()) < 0
}

// Block generates a SeriesBlock based on provided config
func Block(conf BlockConfig) SeriesBlock {
	if conf.NumPoints <= 0 {
		return nil
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	testData := make(SeriesBlock, len(conf.IDs))
	for i, name := range conf.IDs {
		datapoints := make([]ts.Datapoint, 0, conf.NumPoints)
		for j := 0; j < conf.NumPoints; j++ {
			timestamp := conf.Start.Add(time.Duration(j) * time.Second)
			datapoints = append(datapoints, ts.Datapoint{
				Timestamp: timestamp,
				Value:     testgen.GenerateFloatVal(r, 3, 1),
			})
		}
		testData[i] = Series{
			ID:   ts.StringID(name),
			Data: datapoints,
		}
	}
	return testData
}

// BlocksByStart generates a map of SeriesBlocks keyed by Start time
// for the provided configs
func BlocksByStart(confs []BlockConfig) SeriesBlocksByStart {
	seriesMaps := make(map[time.Time]SeriesBlock)
	for _, conf := range confs {
		seriesMaps[conf.Start] = Block(conf)
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
					ID:        blk.ID,
					Datapoint: dp,
				})
			}
		}
	}
	sort.Sort(pointsByTime)
	return pointsByTime
}

// Making SeriesDataPointsByTimes sortable

func (l SeriesDataPointsByTime) Len() int      { return len(l) }
func (l SeriesDataPointsByTime) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l SeriesDataPointsByTime) Less(i, j int) bool {
	if !l[i].Timestamp.Equal(l[j].Timestamp) {
		return l[i].Timestamp.Before(l[j].Timestamp)
	}
	return bytes.Compare(l[i].ID.Data().Get(), l[j].ID.Data().Get()) < 0
}
