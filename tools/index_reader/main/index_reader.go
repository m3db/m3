package main

import "fmt"

import (
	"os"
	"strconv"
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/pool"
	"github.com/pborman/getopt"
)

const (
	defaultBufferReadSize  = 10
	defaultBufferCapacity  = 1024 * 1024 * 1024
	defaultBufferPoolCount = 10
)

func main() {
	var (
		optPathPrefix = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace  = getopt.StringLong("namespace", 'n', "", "Namespace [e.g. metrics]")
		optShard      = getopt.StringLong("shard-id", 's', "", "Shard ID [expected format uint32]")
		optBlockstart = getopt.StringLong("block-start", 'b', "", "Block Start Time [in nsec]")
	)
	getopt.Parse()

	if len(*optPathPrefix) == 0 {
		getopt.Usage()
		os.Exit(1)
	}

	if len(*optNamespace) == 0 {
		getopt.Usage()
		os.Exit(1)
	}

	if len(*optShard) == 0 {
		getopt.Usage()
		os.Exit(1)
	}

	shardID, shardErr := strconv.Atoi(*optShard)
	if shardErr != nil {
		fmt.Println("Unable to parse shard id, uint32 expected, received: ", *optShard)
		fmt.Println("Error: ", shardErr)
		os.Exit(1)
	}

	if len(*optBlockstart) == 0 {
		getopt.Usage()
		os.Exit(1)
	}

	blockstartInt, blockstartErr := strconv.Atoi(*optBlockstart)
	if blockstartErr != nil {
		fmt.Println("Unable to parse block-start time, int64 expected, received: ", *optBlockstart)
		fmt.Println("Error: ", blockstartErr)
		os.Exit(1)
	}

	seeker := fs.NewSeeker(*optPathPrefix, defaultBufferReadSize, pool.NewBytesPool(
		[]pool.Bucket{pool.Bucket{Capacity: defaultBufferCapacity, Count: defaultBufferPoolCount}}, nil))

	err := seeker.Open(ts.StringID(*optNamespace), uint32(shardID), time.Unix(0, int64(blockstartInt)))

	if err != nil {
		fmt.Println("Unable to open file: ", err)
		os.Exit(1)
	}

	fileIds := seeker.GetFileIds()
	if fileIds == nil {
		fmt.Printf("Unable to get ids")
		os.Exit(1)
	}

	for _, id := range fileIds {
		fmt.Println(id)
	}
}
