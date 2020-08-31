// Copyright (c) 2018 Uber Technologies, Inc.
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
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/ident"

	"github.com/pborman/getopt"
)

func main() {
	var (
		optPathPrefix        = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optSrcNamespace      = getopt.StringLong("src-namespace", 'r', "", "Source namespace [e.g. metrics]")
		optTrgNamespace      = getopt.StringLong("trg-namespace", 't', "", "Target namespace [e.g. metrics]")
		optShard             = getopt.Int32Long("shard", 's', -1, "Shard ID. If not provided all shard will be compared. [expected format uint32]")
		optBlockstart        = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		optVerbose           = getopt.BoolLong("verbose", 'v', "Print ids.")
		missingInSource      = 0
		missingInTarget      = 0
		totalMissingInSource = 0
		totalMissingInTarget = 0
		shards               []uint32
	)
	getopt.Parse()

	// rawLogger, err := zap.NewDevelopment()
	// if err != nil {
	// 	log.Fatalf("unable to create logger: %+v", err)
	// }
	//log := rawLogger.Sugar()

	if *optPathPrefix == "" ||
		*optSrcNamespace == "" ||
		*optTrgNamespace == "" ||
		*optBlockstart <= 0 {
		getopt.Usage()
		os.Exit(1)
	}

	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	srcID := ident.StringID(*optSrcNamespace)
	trgID := ident.StringID(*optTrgNamespace)

	if *optShard > -1 {
		shards = []uint32{uint32(*optShard)}
	} else {
		srcShards, err := ioutil.ReadDir(fs.NamespaceDataDirPath(*optPathPrefix, srcID))
		if err != nil {
			log.Fatalf("could not read source shards", err)
		}

		shards = make([]uint32, 0, len(srcShards))
		for _, f := range srcShards {
			if f.IsDir() {
				i, err := strconv.ParseInt(f.Name(), 10, 32)
				if err != nil {
					// most likely something what doesn't belong to this folder
					continue
				}
				shards = append(shards, uint32(i))
			}
		}
	}

	for _, shard := range shards {
		missingInSource = 0
		missingInTarget = 0
		fmt.Printf("Reading shard %d target ids...", shard)
		targetIds, targetBlockSize := readIds(*optPathPrefix, trgID, shard, *optBlockstart)
		fmt.Printf("Read %d ids.\n", len(targetIds))

		blockStart := *optBlockstart
		for blockStart < *optBlockstart+targetBlockSize {
			fmt.Printf("Reading from source namespace block %d...", blockStart)
			srcIds, srcBlockSize := readIds(*optPathPrefix, srcID, shard, blockStart)
			fmt.Printf("Read %d ids.\n", len(srcIds))

			// Checking if all source ids are in target namespace
			for id := range srcIds {
				count, ok := targetIds[id]
				if !ok {
					if *optVerbose {
						fmt.Printf("WARN: %s was found in source but not in target\n", id)
					}
					missingInTarget++
					continue
				}
				targetIds[id] = count + 1
			}

			blockStart += srcBlockSize
		}

		// Final check for existing ids in a target but not in a source namespace
		for id, v := range targetIds {
			if v == 0 {
				if *optVerbose {
					fmt.Printf("WARN: %s was found in target but not in source\n", id)
				}
				missingInSource++
			}
		}
		fmt.Println("------------------")
		fmt.Printf("Shard %d report\n", shard)
		fmt.Printf("Missing IDs in source: %d\n", missingInSource)
		fmt.Printf("Missing IDs in target: %d\n", missingInTarget)
		fmt.Println()

		totalMissingInSource += missingInSource
		totalMissingInTarget += missingInTarget
	}

	fmt.Println("------------------")
	fmt.Println("Report for all shards")
	fmt.Printf("Total missing IDs in source: %d\n", totalMissingInSource)
	fmt.Printf("Total missing IDs in target: %d\n", totalMissingInTarget)
}

func readIds(pathPrefix string, ns ident.ID, shard uint32, blockStart int64) (map[string]int, int64) {
	trgFiles, err := fs.DataFiles(pathPrefix, ns, shard)
	if err != nil {
		log.Fatalf("could not read source shard data files", err)
	}

	trgFile, ok := trgFiles.LatestVolumeForBlock(time.Unix(0, blockStart))
	if !ok {
		log.Fatalf("couldn't find targets latest volume", err)
	}

	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()
	fsOpts := fs.NewOptions().SetFilePathPrefix(pathPrefix)
	reader, err := fs.NewReader(bytesPool, fsOpts)
	if err != nil {
		log.Fatalf("could not create new reader: %v", err)
	}
	openOpts := fs.DataReaderOpenOptions{
		Identifier: trgFile.ID,
	}
	err = reader.Open(openOpts)
	defer reader.Close()
	if err != nil {
		log.Fatalf("unable to open reader: %v", err)
	}

	ids := make(map[string]int)
	for {
		id, _, _, _, err := reader.ReadMetadata()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("err reading metadata: %v", err)
		}
		ids[id.String()] = 0
	}
	return ids, reader.Range().End.UnixNano() - reader.Range().Start.UnixNano()
}
