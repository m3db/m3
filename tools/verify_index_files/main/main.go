package main

import (
	"flag"
	"io"
	"io/ioutil"
	"log"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/tools"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/pool"
)

var flagParser = flag.NewFlagSet("Verify Index files", flag.ExitOnError)

// TODO: Delete me
// defaultDataReaderBufferSize is the default buffer size for reading TSDB data and index files
const defaultDataReaderBufferSize = 65536

// defaultInfoReaderBufferSize is the default buffer size for reading TSDB info, checkpoint and digest files
const defaultInfoReaderBufferSize = 64

type seriesChecksums struct {
	host   string
	shard  uint32
	block  int64
	series seriesMap
}

type seriesMap map[string]series

type checksum uint32

type series struct {
	name     string
	checksum checksum
	offset   int64
}

var (
	pathPrefixArg       = flagParser.String("path-prefix", "/var/lib/m3db", "Path prefix - must contain a folder called 'commitlogs'")
	namespaceArg        = flagParser.String("namespace", "metrics", "Namespace")
	shardsArg           = flagParser.String("shards", "", "Shards - set comma separated list of shards")
	blocksArgs          = flagParser.String("blocks", "", "Start unix timestamp (Seconds) - set comma separated list of unix timestamps")
	compareChecksumsArg = flagParser.Bool("compare-checksums", true, "Compare checksums")
)

var bytesPool pool.CheckedBytesPool

func main() {
	flagParser.Parse(os.Args[1:])

	var (
		pathPrefix       = *pathPrefixArg
		namespaceStr     = *namespaceArg
		shardsVal        = *shardsArg
		blocksVal        = *blocksArgs
		compareChecksums = *compareChecksumsArg
	)

	blocks := parseBlockArgs(blocksVal)
	hosts, err := ioutil.ReadDir(pathPrefix)
	if err != nil {
		log.Fatalf("Err reading dir: %s, err: %s\n", pathPrefix, err)
	}

	shards := parseShards(shardsVal)
	for _, block := range blocks {
		for _, shard := range shards {
			log.Printf("Running test for shard: %d\n", shard)
			// var baseSeriesChecksums seriesChecksums
			allHostSeriesChecksumsForShard := []seriesChecksums{}
			// Accumulate all the series checksums for each host for this shard
			for _, host := range hosts {
				hostShardReader, err := newReader(namespaceStr, pathPrefix, host.Name(), shard, time.Unix(block, 0))
				if err != nil {
					// Ignore folders for hosts that don't have this data
					if err.Error() == "checkpoint file does not exist" {
						continue
					}
					log.Fatalf("Err creating new reader: %s\n", err.Error())
				}
				hostShardSeriesChecksums := seriesChecksumsFromReader(hostShardReader, host.Name(), shard, block)
				allHostSeriesChecksumsForShard = append(allHostSeriesChecksumsForShard, hostShardSeriesChecksums)
			}

			allHostSeriesMapsForShard := []seriesMap{}
			for _, seriesChecksum := range allHostSeriesChecksumsForShard {
				allHostSeriesMapsForShard = append(allHostSeriesMapsForShard, seriesChecksum.series)
			}
			allSeriesForShard := mergeMaps(allHostSeriesMapsForShard...)
			for _, hostSeriesForShard := range allHostSeriesChecksumsForShard {
				compareSeriesChecksums(allSeriesForShard, hostSeriesForShard, compareChecksums)
			}
		}
	}
}

func seriesChecksumsFromReader(reader fs.FileSetReader, host string, shard uint32, block int64) seriesChecksums {
	seriesMap := seriesMap{}
	seriesChecksums := seriesChecksums{
		host:   host,
		series: seriesMap,
		shard:  shard,
		block:  block,
	}
	for {
		id, _, checksumVal, offset, err := reader.ReadMetadata()
		if err == io.EOF {
			return seriesChecksums
		}
		if err != nil {
			log.Fatal("Err reading from reader: ", err.Error())
		}
		idString := id.String()
		seriesMap[idString] = series{
			name:     idString,
			checksum: checksum(checksumVal),
			offset:   offset,
		}
	}
}

func compareSeriesChecksums(against seriesMap, evaluate seriesChecksums, compareChecksums bool) {
	againstMap := against
	evaluateMap := evaluate.series
	missingSeries := []series{}
	checksumMismatchSeries := []series{}

	if len(againstMap) == len(evaluateMap) {
		log.Printf(
			"Host %s has all %d series for shard: %d and block: %d",
			evaluate.host, len(againstMap), evaluate.shard, evaluate.block,
		)
	} else {
		log.Printf(
			"Host %s has %d series, but there are a total of %d series in shard: %d and block %d\n",
			evaluate.host, len(evaluateMap), len(againstMap), evaluate.shard, evaluate.block,
		)
	}

	for seriesName, againstSeries := range againstMap {
		evaluateSeries, ok := evaluateMap[seriesName]

		if !ok {
			missingSeries = append(missingSeries, againstSeries)
			continue
		}

		if compareChecksums && againstSeries.checksum != evaluateSeries.checksum {
			checksumMismatchSeries = append(checksumMismatchSeries, againstSeries)
		}
	}

	for _, missing := range missingSeries {
		log.Printf("Host %s is missing %s with offset: %d\n", evaluate.host, missing.name, missing.offset)
	}

	for _, mismatch := range checksumMismatchSeries {
		log.Printf("Host %s has mismatching checksum for %s with offset: %d\n", evaluate.host, mismatch.name, mismatch.offset)
	}
}

func mergeMaps(seriesMaps ...seriesMap) seriesMap {
	merged := seriesMap{}
	for _, sMap := range seriesMaps {
		for key, val := range sMap {
			merged[key] = val
		}
	}
	return merged
}

func newReader(namespace, pathPrefix, hostName string, shard uint32, start time.Time) (fs.FileSetReader, error) {
	reader := fs.NewReader(path.Join(pathPrefix, hostName), defaultDataReaderBufferSize, defaultInfoReaderBufferSize, bytesPool, msgpack.NewDecodingOptions())
	err := reader.Open(ts.StringID(namespace), shard, start)
	return reader, err
}

func parseShards(shards string) []uint32 {
	var allShards []uint32

	if strings.TrimSpace(shards) == "" {
		return []uint32{}
	}

	// Handle commda-delimited shard list 1,3,5, etc
	for _, shard := range strings.Split(shards, ",") {
		shard = strings.TrimSpace(shard)
		if shard == "" {
			log.Fatalf("Invalid shard list: '%s'", shards)
		}
		value, err := strconv.Atoi(shard)
		if err != nil {
			log.Fatalf("could not parse shard '%s': %v", shard, err)
		}
		allShards = append(allShards, uint32(value))
	}

	return allShards
}

func parseBlockArgs(blocks string) []int64 {
	allBlocks := []int64{}

	for _, block := range strings.Split(blocks, ",") {
		block = strings.TrimSpace(block)
		if block == "" {
			log.Fatalf("Invalid block list: '%s'", block)
		}
		value, err := strconv.Atoi(block)
		if err != nil {
			log.Fatalf("could not parse blocks '%s': %v", block, err)
		}
		allBlocks = append(allBlocks, int64(value))
	}

	return allBlocks
}

func init() {
	log.SetOutput(os.Stdout)
	log.Println("Initializing bytes pool...")
	bytesPool = tools.NewCheckedBytesPool()
}
