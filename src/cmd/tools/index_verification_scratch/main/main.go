// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/vellum"

	"net/http"
	_ "net/http/pprof"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

const snapshotType = "snapshot"
const flushType = "flush"

type shardDescription struct {
	isVellum    bool
	shardID     uint32
	seriesCount int
	indexSize   int64
	bytesSize   int64
	vellumSize  int64
}

type verificationOpts struct {
	snappy            bool
	volume            int
	blockStart        time.Time
	ns                ident.ID
	fsOpts            fs.Options
	fsType            persist.FileSetType
	shardDescriptions []shardDescription
	bytesPool         pool.CheckedBytesPool
}

func (s shardDescription) String() string {
	if s.isVellum {
		return fmt.Sprintf("Shard %d: series count: %d, index size: %d, vellum size: %d, ratio: %v",
			s.shardID, s.seriesCount, s.indexSize, s.vellumSize, float64(s.vellumSize)/float64(s.indexSize))
	}

	return fmt.Sprintf("Shard %d: series count: %d, index size: %d, bytes size: %d, ratio: %v",
		s.shardID, s.seriesCount, s.indexSize, s.bytesSize, float64(s.bytesSize)/float64(s.indexSize))
}

func main() {
	var (
		optPathPrefix  = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace   = getopt.StringLong("namespace", 'n', "default", "Namespace [e.g. metrics]")
		optBlockstart  = getopt.Int64Long("block-start", 'b', 1596009600000000000, "Block Start Time [in nsec]")
		volume         = getopt.Int64Long("volume", 'v', 0, "Volume number")
		fileSetTypeArg = getopt.StringLong("fileset-type", 'f', flushType, fmt.Sprintf("%s|%s", flushType, snapshotType))
		pVellum        = getopt.Bool('u', "use vellum")
		pSnappy        = getopt.Bool('s', "use snappy (only for vellum)")
	)
	getopt.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	log := rawLogger.Sugar()

	vellum, snappy := *pVellum, *pSnappy
	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optBlockstart <= 0 ||
		*volume < 0 ||
		!vellum && snappy ||
		(*fileSetTypeArg != snapshotType && *fileSetTypeArg != flushType) {
		getopt.Usage()
		os.Exit(1)
	}

	shardDescriptions := make([]shardDescription, 0, 10)
	root := path.Join(*optPathPrefix, "data", *optNamespace)
	err = filepath.Walk(root, func(filename string, info os.FileInfo, err error) error {
		if strings.Contains(filename, "index") {
			d := path.Base(path.Dir(filename))
			idx, err := strconv.Atoi(d)
			if err != nil {
				log.Fatalf("error parsing shard name: %v", err)
			}

			shardDescriptions = append(shardDescriptions, shardDescription{
				isVellum:  vellum,
				shardID:   uint32(idx),
				indexSize: info.Size(),
			})
		}

		return nil
	})

	if err != nil {
		log.Fatalf("error iterating file sets: %v", err)
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

	opts := verificationOpts{
		snappy:            snappy,
		volume:            int(*volume),
		blockStart:        time.Unix(0, *optBlockstart),
		ns:                ident.StringID(*optNamespace),
		fsOpts:            fs.NewOptions().SetFilePathPrefix(*optPathPrefix),
		fsType:            fileSetType,
		shardDescriptions: shardDescriptions,
		bytesPool:         bytesPool,
	}

	readStart := time.Now()
	if !vellum {
		indexVerificationSummary(opts)
	} else {
		indexVerificationVellum(opts)
	}

	totalCount := 0
	var totalIndex, outputSize int64
	for _, shard := range shardDescriptions {
		totalIndex += shard.indexSize
		if shard.isVellum {
			outputSize += shard.vellumSize
		} else {
			outputSize += shard.bytesSize
		}
		totalCount += shard.seriesCount
	}

	fmt.Println("In total:")
	fmt.Println("Using vellum:", vellum)
	if vellum {
		fmt.Println("Using snappy:", snappy)
	}

	fmt.Printf("%d series, %d index size, %d output size, %v ratio\n",
		totalCount, totalIndex, outputSize, float64(outputSize)/float64(totalIndex))
	fmt.Println("Took: ", time.Since(readStart))
}

func indexVerificationSummary(opts verificationOpts) {
	for i, shard := range opts.shardDescriptions {
		summarizer, err := fs.NewSummarizer(opts.fsOpts)
		if err != nil {
			log.Fatalf("could not create new summarizer: %v", err)
		}

		openOpts := fs.DataReaderOpenOptions{
			OrderedByIndex: true,
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   opts.ns,
				Shard:       shard.shardID,
				BlockStart:  opts.blockStart,
				VolumeIndex: opts.volume,
			},
			FileSetType: opts.fsType,
		}

		err = summarizer.Open(openOpts)
		if err != nil {
			log.Fatalf("unable to open summarizer: %v", err)
		}

		count := 0
		for {
			_, err := summarizer.ReadMetadata()
			if err != nil {
				if err != io.EOF {
					log.Fatalf("reading failure: %v", err)
				}

				break
			}

			count++
		}

		if err := summarizer.Close(); err != nil {
			log.Fatalf("cannot close reader: %v", err)
		}

		opts.shardDescriptions[i].seriesCount = count
		opts.shardDescriptions[i].bytesSize = int64(count * 12)

		fmt.Println(i, "/", len(opts.shardDescriptions), ":", opts.shardDescriptions[i])
	}
}

type noteSizeWriter struct {
	size int64
}

func (n *noteSizeWriter) reset() { n.size = 0 }

func (n *noteSizeWriter) Write(p []byte) (int, error) {
	l := len(p)
	n.size += int64(l)
	return l, nil
}

func indexVerificationVellum(opts verificationOpts) {
	builderOpts := &vellum.BuilderOpts{
		Encoder:                  1,
		RegistryTableSize:        10000, // 10k
		RegistryMRUSize:          4,     // 4
		UnfinishedNodesStackSize: 4096,
		BuilderNodePoolingConfig: vellum.BuilderNodePoolingConfig{
			MaxSize:           2 << 16, // ~130k
			MaxTransitionSize: 2 << 7,  // 256
		},
	}

	for i, shard := range opts.shardDescriptions {
		reader, err := fs.NewReader(opts.bytesPool, opts.fsOpts)
		if err != nil {
			log.Fatalf("could not create new reader: %v", err)
		}

		openOpts := fs.DataReaderOpenOptions{
			OrderedByIndex: true,
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   opts.ns,
				Shard:       shard.shardID,
				BlockStart:  opts.blockStart,
				VolumeIndex: opts.volume,
			},
			FileSetType: opts.fsType,
		}

		err = reader.Open(openOpts)
		if err != nil {
			log.Fatalf("unable to open reader: %v", err)
		}

		// f, err := ioutil.TempFile(os.TempDir(),
		// 	fmt.Sprintf("vellum_%d.txt", shard.shardID))
		// if err != nil {
		// 	log.Fatalf("unable to create output file: %v", err)
		// }

		// defer os.Remove(f.Name())

		sizeWriter := &noteSizeWriter{}
		var w io.Writer = sizeWriter
		if opts.snappy {
			w = snappy.NewWriter(sizeWriter)
		}

		builder, err := vellum.New(w, builderOpts)
		if err != nil {
			log.Fatalf("unable to create vellum builder: %v", err)
		}

		count := 0
		for {
			id, _, _, checksum, err := reader.Read()
			if err != nil {
				if err != io.EOF {
					log.Fatalf("reading failure: %v", err)
				}

				break
			}

			if err := builder.Insert(id.Bytes(), uint64(checksum)); err != nil {
				log.Fatalf("cannot insert into builder: %v", err)
			}

			count++
		}

		if err := builder.Close(); err != nil {
			log.Fatalf("cannot close builder: %v", err)
		}

		if err := reader.Close(); err != nil {
			log.Fatalf("cannot close reader: %v", err)
		}

		// stat, err := f.Stat()
		// if err != nil {
		// 	log.Fatalf("error getting file stats: %v", err)
		// }

		opts.shardDescriptions[i].seriesCount = count
		opts.shardDescriptions[i].vellumSize = sizeWriter.size

		fmt.Println(i, "/", len(opts.shardDescriptions), ":", opts.shardDescriptions[i])
		// if err := f.Close(); err != nil {
		// 	log.Fatalf("error closing file: %v", err)
		// }
	}
}
