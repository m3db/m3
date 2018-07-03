package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/m3db/m3db/src/cmd/tools"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/persist"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/storage/block"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"

	"github.com/pborman/getopt"
)

func main() {
	var (
		optPathPrefix = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace  = getopt.StringLong("namespace", 'n', "", "Namespace [e.g. metrics]")
		optShard      = getopt.Uint32Long("shard", 's', 0, "Shard [expected format uint32]")
		optBlockstart = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		volume        = getopt.Int64Long("volume", 'v', -1, "Volume number")
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

	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   ident.StringID(*optNamespace),
			Shard:       *optShard,
			BlockStart:  time.Unix(0, *optBlockstart),
			VolumeIndex: int(*volume),
		},
		FileSetType: persist.FileSetSnapshotType,
	}

	err = reader.Open(openOpts)
	if err != nil {
		log.Fatalf("unable to open reader: %v", err)
	}

	iter := encoding.NewMultiReaderIterator(func(reader io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(reader, true, encodingOpts)
	}, nil)

	for {
		id, _, data, _, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("err reading metadata: %v", err)
		}

		block := block.NewDatabaseBlock(time.Unix(0, *optBlockstart), 2*time.Hour, ts.NewSegment(data, nil, ts.FinalizeHead), block.NewOptions())
		block.Reset(time.Unix(0, *optBlockstart), 2*time.Hour, ts.NewSegment(data, nil, ts.FinalizeHead))

		stream, err := block.Stream(context.NewContext())
		if err != nil {
			log.Fatal(err.Error())
		}

		iter.Reset([]xio.SegmentReader{stream}, time.Time{}, 0)
		// iter := m3tsz.NewReaderIterator(stream, true, encodingOpts)
		for iter.Next() {
			dp, _, _ := iter.Current()
			// Use fmt package so it goes to stdout instead of stderr
			fmt.Printf("{id: %s, dp: %+v}\n", id.String(), dp)
		}
		if err := iter.Err(); err != nil {
			log.Fatalf("unable to iterate original data: %v", err)
		}
		iter.Close()
	}
}
