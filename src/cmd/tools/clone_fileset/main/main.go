package main

import (
	"flag"
	"os"

	"github.com/m3db/m3/src/dbnode/persist/fs/clone"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

var (
	optSrcPathPrefix  = flag.String("src-path-prefix", "/var/lib/m3db", "Source Path prefix")
	optSrcNamespace   = flag.String("src-namespace", "metrics", "Source Namespace")
	optSrcShard       = flag.Uint("src-shard", 0, "Source Shard ID")
	optSrcBlockstart  = flag.Int64("src-block-start", 0, "Source Block Start Time [in nsec]")
	optDestPathPrefix = flag.String("dest-path-prefix", "/tmp/m3db-copy", "Destination Path prefix")
	optDestNamespace  = flag.String("dest-namespace", "metrics", "Destination Namespace")
	optDestShard      = flag.Uint("dest-shard", 0, "Destination Shard ID")
	optDestBlockstart = flag.Int64("dest-block-start", 0, "Destination Block Start Time [in nsec]")
	optDestBlockSize  = flag.Duration("dest-block-size", 0, "Destination Block Size")
)

func main() {
	flag.Parse()
	if *optSrcPathPrefix == "" ||
		*optDestPathPrefix == "" ||
		*optSrcNamespace == "" ||
		*optDestNamespace == "" ||
		*optSrcBlockstart <= 0 ||
		*optDestBlockstart <= 0 {
		flag.Usage()
		os.Exit(1)
	}

	log := xlog.NewLogger(os.Stderr)
	src := clone.FileSetID{
		PathPrefix: *optSrcPathPrefix,
		Namespace:  *optSrcNamespace,
		Shard:      uint32(*optSrcShard),
		Blockstart: xtime.FromNanoseconds(*optSrcBlockstart),
	}
	dest := clone.FileSetID{
		PathPrefix: *optDestPathPrefix,
		Namespace:  *optDestNamespace,
		Shard:      uint32(*optDestShard),
		Blockstart: xtime.FromNanoseconds(*optDestBlockstart),
	}

	log.Infof("source: %+v", src)
	log.Infof("destination: %+v", dest)

	opts := clone.NewOptions()
	cloner := clone.New(opts)
	if err := cloner.Clone(src, dest, *optDestBlockSize); err != nil {
		log.Fatalf("unable to clone: %v", err)
	}

	log.Infof("successfully cloned data")
}
