package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/m3db/m3db/src/cmd/tools"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/persist"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"

	"github.com/pborman/getopt"
)

const snapshotType = "snapshot"
const flushType = "flush"

func main() {
	var (
		optPathPrefix  = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace   = getopt.StringLong("namespace", 'n', "", "Namespace [e.g. metrics]")
		optShard       = getopt.Uint32Long("shard", 's', 0, "Shard [expected format uint32]")
		optBlockstart  = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		volume         = getopt.Int64Long("volume", 'v', 0, "Volume number")
		fileSetTypeArg = getopt.StringLong("fileset-type", 't', flushType, fmt.Sprintf("%s|%s", flushType, snapshotType))
		idFilter       = getopt.StringLong("id-filter", 'f', "", "ID Contains Filter (optional)")
		log            = xlog.NewLogger(os.Stderr)
	)
	getopt.Parse()

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optShard < 0 ||
		*optBlockstart <= 0 ||
		*volume < 0 ||
		(*fileSetTypeArg != snapshotType && *fileSetTypeArg != flushType) {
		getopt.Usage()
		os.Exit(1)
	}

	var fileSetType persist.FileSetType
	switch *fileSetTypeArg {
	case flushType:
		fileSetType = persist.FileSetFlushType
	case snapshotType:
		fileSetType = persist.FileSetSnapshotType
	default:
		log.Fatalf("unknown fileset type: %s", *fileSetTypeArg)
	}

	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	encodingOpts := encoding.NewOptions().SetBytesPool(bytesPool)

	fsOpts := fs.NewOptions().SetFilePathPrefix(*optPathPrefix)
	reader, err := fs.NewReader(bytesPool, fsOpts)
	if err != nil {
		log.Fatalf("could not create new reader: %v", err)
	}

	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   ident.StringID(*optNamespace),
			Shard:       *optShard,
			BlockStart:  time.Unix(0, *optBlockstart),
			VolumeIndex: int(*volume),
		},
		FileSetType: fileSetType,
	}

	err = reader.Open(openOpts)
	if err != nil {
		log.Fatalf("unable to open reader: %v", err)
	}

	for {
		id, _, data, _, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("err reading metadata: %v", err)
		}

		if *idFilter != "" && !strings.Contains(id.String(), *idFilter) {
			continue
		}

		data.IncRef()
		iter := m3tsz.NewReaderIterator(bytes.NewReader(data.Bytes()), true, encodingOpts)
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
