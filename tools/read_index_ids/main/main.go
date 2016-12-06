package main

import "fmt"

import (
	"os"
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/ts"
	xlog "github.com/m3db/m3x/log"
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
		optShard      = getopt.Uint32Long("shard-id", 's', 0, "Shard ID [expected format uint32]")
		optBlockstart = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		log           = xlog.NewLogger(os.Stderr)
	)
	getopt.Parse()

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optShard < 0 ||
		*optBlockstart <= 0 {
		getopt.Usage()
		os.Exit(1)
	}

	seeker := fs.NewSeeker(*optPathPrefix, defaultBufferReadSize, pool.NewBytesPool(
		[]pool.Bucket{pool.Bucket{Capacity: defaultBufferCapacity, Count: defaultBufferPoolCount}}, nil))

	err := seeker.Open(ts.StringID(*optNamespace), uint32(*optShard), time.Unix(0, int64(*optBlockstart)))

	if err != nil {
		log.Fatalf("Unable to open file: %v", err)
	}

	fileIds := seeker.IDs()
	if fileIds == nil {
		log.Fatalf("Unable to retrieve ids")
	}

	for _, id := range fileIds {
		fmt.Println(id.String())
	}
}
