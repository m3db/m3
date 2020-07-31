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
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
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
	"github.com/m3db/vellum"

	"net/http"
	_ "net/http/pprof"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

const snapshotType = "snapshot"
const flushType = "flush"

type shardDescription struct {
	shardID     uint32
	seriesCount int
	indexSize   int64
	vellumSize  int64
}

func (s shardDescription) String() string {
	return fmt.Sprintf("Shard %d: series count: %d, index size: %d, vellum size: %d, ratio: %v",
		s.shardID, s.seriesCount, s.indexSize, s.vellumSize, float64(s.vellumSize)/float64(s.indexSize))
}

func main() {
	var (
		optPathPrefix  = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace   = getopt.StringLong("namespace", 'n', "default", "Namespace [e.g. metrics]")
		optBlockstart  = getopt.Int64Long("block-start", 'b', 1596009600000000000, "Block Start Time [in nsec]")
		volume         = getopt.Int64Long("volume", 'v', 0, "Volume number")
		fileSetTypeArg = getopt.StringLong("fileset-type", 'f', flushType, fmt.Sprintf("%s|%s", flushType, snapshotType))
		noSnappy       = getopt.Bool('s', "disable snappy")
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

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optBlockstart <= 0 ||
		*volume < 0 ||
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

	var (
		fsOpts = fs.NewOptions().SetFilePathPrefix(*optPathPrefix)
	)

	for i, shard := range shardDescriptions {
		reader, err := fs.NewReader(bytesPool, fsOpts)
		if err != nil {
			log.Fatalf("could not create new reader: %v", err)
		}

		openOpts := fs.DataReaderOpenOptions{
			OrderedByIndex: true,
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   ident.StringID(*optNamespace),
				Shard:       shard.shardID,
				BlockStart:  time.Unix(0, *optBlockstart),
				VolumeIndex: int(*volume),
			},
			FileSetType: fileSetType,
		}

		err = reader.Open(openOpts)
		if err != nil {
			log.Fatalf("unable to open reader: %v", err)
		}

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

		f, err := ioutil.TempFile(os.TempDir(),
			fmt.Sprintf("vellum_%d.txt", shard.shardID))
		if err != nil {
			log.Fatalf("unable to create output file: %v", err)
		}

		defer os.Remove(f.Name())

		var w io.Writer
		if *noSnappy {
			w = bufio.NewWriter(f)
		} else {
			w = snappy.NewWriter(f)
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

		stat, err := f.Stat()
		if err != nil {
			log.Fatalf("error getting file stats: %v", err)
		}

		shardDescriptions[i].seriesCount = count
		shardDescriptions[i].vellumSize = stat.Size()

		fmt.Println(i, "/", len(shardDescriptions), ":", shardDescriptions[i])
		if err := f.Close(); err != nil {
			log.Fatalf("error closing file: %v", err)
		}
	}

	totalCount := 0
	var totalIndex, totalVellum int64
	for _, shard := range shardDescriptions {
		totalIndex += shard.indexSize
		totalVellum += shard.vellumSize
		totalCount += shard.seriesCount
	}

	fmt.Println("In total:")
	fmt.Printf("%d series, %d index size, %d vellum size, %v ratio\n",
		totalCount, totalIndex, totalVellum, float64(totalVellum)/float64(totalIndex))
}
