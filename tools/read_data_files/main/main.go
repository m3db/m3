package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/tools"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"

	"github.com/pborman/getopt"
)

func main() {
	var (
		optPathPrefix = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace  = getopt.StringLong("namespace", 'n', "", "Namespace [e.g. metrics]")
		optShard      = getopt.Uint32Long("shard-id", 's', 0, "Shard ID [expected format uint32]")
		optBlockstart = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		idFilter      = getopt.StringLong("id-filter", 'f', "", "ID Contains Filter")
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

	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	encodingOpts := encoding.NewOptions().SetBytesPool(bytesPool)

	fsOpts := fs.NewOptions().SetFilePathPrefix(*optPathPrefix)
	reader, err := fs.NewReader(bytesPool, fsOpts)
	if err != nil {
		log.Fatalf("could not create new reader: %v", err)
	}
	err = reader.Open(ident.StringID(*optNamespace), *optShard, time.Unix(0, *optBlockstart))
	if err != nil {
		log.Fatalf("unable to open reader: %v", err)
	}

	for {
		id, data, _, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("err reading metadata: %v", err)
		}

		if !strings.Contains(id.String(), *idFilter) {
			continue
		}

		data.IncRef()
		iter := m3tsz.NewReaderIterator(bytes.NewReader(data.Get()), true, encodingOpts)
		for iter.Next() {
			dp, _, _ := iter.Current()
			// Use fmt package so it goes to stdout instead of stderr
			fmt.Printf("{id: %s, dp: %+v}\n", id.String(), dp)
		}
		if err := iter.Err(); err != nil {
			log.Fatalf("unable to iterate original data: %v", err)
		}
		iter.Close()

		data.DecRef()
		data.Finalize()
	}
}
