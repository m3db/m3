package main

import (
	"flag"
	"io"
	"os"

	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/ts"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

const (
	defaultBufferSize = 65536
)

var (
	optSrcPathPrefix  = flag.String("src-path-prefix", "/var/lib/m3db", "Source Path prefix")
	optSrcNamespace   = flag.String("src-namespace", "metrics", "Source Namespace")
	optSrcShard       = flag.Uint("src-shard-id", 0, "Source Shard ID")
	optSrcBlockstart  = flag.Int64("src-block-start", 0, "Source Block Start Time [in nsec]")
	optDestPathPrefix = flag.String("dest-path-prefix", "/tmp/m3db-copy", "Destination Path prefix")
	optDestNamespace  = flag.String("dest-namespace", "metrics", "Destination Namespace")
	optDestShard      = flag.Uint("dest-shard-id", 0, "Destination Shard ID")
	optDestBlockstart = flag.Int64("dest-block-start", 0, "Destination Block Start Time [in nsec]")
	optDestBlockSize  = flag.Duration("dest-block-size", 0, "Destination Block Size")
)

var (
	targetFileMode = os.FileMode(0666)
	targetDirMode  = os.ModeDir | os.FileMode(0755)
	log            = xlog.NewLogger(os.Stderr)
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

	log.Infof("source: [ path-prefix = %s, namespace = %s, shard-id = %d, block-start = %d ]", *optSrcPathPrefix, *optSrcNamespace, *optSrcShard, *optSrcBlockstart)
	reader := fs.NewReader(*optSrcPathPrefix, defaultBufferSize, nil, msgpack.NewDecodingOptions())
	if err := reader.Open(ts.StringID(*optSrcNamespace), uint32(*optSrcShard), xtime.FromNanoseconds(*optSrcBlockstart)); err != nil {
		log.Fatalf("unable to read source fileset: %v", err)
	}

	log.Infof("destination: [ path-prefix = %s, namespace = %s, shard-id = %d, block-start = %d ]", *optDestPathPrefix, *optDestNamespace, *optDestShard, *optDestBlockstart)
	writer := fs.NewWriter(*optDestBlockSize, *optDestPathPrefix, defaultBufferSize, targetFileMode, targetDirMode)
	if err := writer.Open(ts.StringID(*optDestNamespace), uint32(*optDestShard), xtime.FromNanoseconds(*optDestBlockstart)); err != nil {
		log.Fatalf("unable to open fileset writer: %v", err)
	}

	for {
		id, data, checksum, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("unexpected error while reading data: %v", err)
		}

		data.IncRef()
		if err := writer.Write(id, data, checksum); err != nil {
			log.Fatalf("unexpected error while writing data: %v", err)
		}
		data.DecRef()
		data.Finalize()
	}

	if err := writer.Close(); err != nil {
		log.Fatalf("unable to finalize writer: %v", err)
	}

	if err := reader.Close(); err != nil {
		log.Fatalf("unable to finalize reader: %v", err)
	}

	log.Infof("successfully cloned data")
}
