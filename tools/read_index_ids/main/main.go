package main

import (
	"io"
	"os"
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"

	"github.com/pborman/getopt"
)

const (
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

	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{pool.Bucket{
		Capacity: defaultBufferCapacity,
		Count:    defaultBufferPoolCount,
	}}, nil, func(buckets []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(buckets, nil)
	})
	bytesPool.Init()

	reader, err := fs.NewReader(bytesPool, fs.NewOptions())
	if err != nil {
		log.Fatalf("could not create new reader: %v", err)
	}
	err = reader.Open(ident.StringID(*optNamespace), *optShard, time.Unix(*optBlockstart, 0))
	if err != nil {
		log.Fatalf("unable to open reader: %v", err)
	}

	for {
		id, _, _, err := reader.ReadMetadata()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("err reading metadata: %v", err)
		}
		log.Info(id.String())
	}
}
