package generate

import (
	"bytes"
	"math/rand"
	"sort"
	"time"

	"github.com/m3db/m3db/encoding/testgen"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

// Making SeriesBlock sortable
func (l SeriesBlock) Len() int      { return len(l) }
func (l SeriesBlock) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l SeriesBlock) Less(i, j int) bool {
	return bytes.Compare(l[i].ID.Data().Bytes(), l[j].ID.Data().Bytes()) < 0
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
			ID:   ident.StringID(name),
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
		seriesMaps[xtime.ToUnixNano(conf.Start)] = Block(conf)
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
	if !l[i].Timestamp.Equal(l[j].Timestamp) {
		return l[i].Timestamp.Before(l[j].Timestamp)
	}
	return bytes.Compare(l[i].ID.Data().Bytes(), l[j].ID.Data().Bytes()) < 0
}
