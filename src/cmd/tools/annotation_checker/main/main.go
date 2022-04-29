// Copyright (c) 2021 Uber Technologies, Inc.
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
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

const (
	snapshotType = "snapshot"
	flushType    = "flush"

	allShards = -1
)

func main() {
	var (
		optPathPrefix  = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		fileSetTypeArg = getopt.StringLong("fileset-type", 't', flushType, fmt.Sprintf("%s|%s", flushType, snapshotType))
		optNamespace   = getopt.StringLong("namespace", 'n', "default", "Namespace [e.g. metrics]")
		optShard       = getopt.IntLong("shard", 's', allShards,
			fmt.Sprintf("Shard number, or %v for all shards in the directory", allShards))
		optBlockstart = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		volume        = getopt.Int64Long("volume", 'v', 0, "Volume number")

		annotationRewrittenFilter = getopt.BoolLong("annotation-rewritten", 'R', "Filters metrics with annotation rewrites")
	)
	getopt.Parse()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	log := rawLogger.Sugar()

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optShard < allShards ||
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

	shards := []uint32{uint32(*optShard)}
	if *optShard == allShards {
		shards, err = getShards(*optPathPrefix, fileSetType, *optNamespace)
		if err != nil {
			log.Fatalf("failed extracting the list of shards: %v", err)
		}
	}

	// Not using bytes pool with streaming reads/writes to avoid the fixed memory overhead.
	var bytesPool pool.CheckedBytesPool
	encodingOpts := encoding.NewOptions().SetBytesPool(bytesPool)

	fsOpts := fs.NewOptions().SetFilePathPrefix(*optPathPrefix)
	reader, err := fs.NewReader(bytesPool, fsOpts)
	if err != nil {
		log.Fatalf("could not create new reader: %v", err)
	}

	metricsMap := make(map[string]struct{})

	for _, shard := range shards {
		openOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   ident.StringID(*optNamespace),
				Shard:       shard,
				BlockStart:  xtime.UnixNano(*optBlockstart),
				VolumeIndex: int(*volume),
			},
			FileSetType:      fileSetType,
			StreamingEnabled: true,
		}

		err = reader.Open(openOpts)
		if err != nil {
			log.Fatalf("unable to open reader for shard %v: %v", shard, err)
		}

		for {
			entry, err := reader.StreamingRead()
			if xerrors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				log.Fatalf("err reading metadata: %v", err)
			}

			noInitialAnnotation, annotationRewritten, err := checkAnnotations(entry.Data, encodingOpts)
			if err != nil {
				log.Fatalf("failed checking annotations: %v", err)
			}

			if (!*annotationRewrittenFilter && noInitialAnnotation) || (*annotationRewrittenFilter && annotationRewritten) {
				metricsMap[entry.ID.String()] = struct{}{}
			}
		}

		if err := reader.Close(); err != nil {
			log.Fatalf("unable to close reader: %v", err)
		}
	}

	metrics := make([]string, 0)
	for m := range metricsMap {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)
	for _, metric := range metrics {
		fmt.Println(metric) // nolint: forbidigo
	}
}

func checkAnnotations(data []byte, encodingOpts encoding.Options) (bool, bool, error) {
	iter := m3tsz.NewReaderIterator(xio.NewBytesReader64(data), true, encodingOpts)
	defer iter.Close()

	var (
		previousAnnotationBase64 *string
		noInitialAnnotation      = true
		annotationRewritten      = false

		firstDatapoint = true
	)

	for iter.Next() {
		_, _, annotation := iter.Current()
		if len(annotation) > 0 {
			if firstDatapoint {
				noInitialAnnotation = false
			}
			annotationBase64 := base64.StdEncoding.EncodeToString(annotation)
			if previousAnnotationBase64 != nil && *previousAnnotationBase64 != annotationBase64 {
				annotationRewritten = true
			}
			previousAnnotationBase64 = &annotationBase64
		}
		firstDatapoint = false
	}
	if err := iter.Err(); err != nil {
		return false, false, fmt.Errorf("unable to iterate original data: %w", err)
	}

	return noInitialAnnotation, annotationRewritten, nil
}

func getShards(pathPrefix string, fileSetType persist.FileSetType, namespace string) ([]uint32, error) {
	nsID := ident.StringID(namespace)
	path := fs.NamespaceDataDirPath(pathPrefix, nsID)
	if fileSetType == persist.FileSetSnapshotType {
		path = fs.NamespaceSnapshotsDirPath(pathPrefix, nsID)
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading namespace directory: %w", err)
	}

	shards := make([]uint32, 0)
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		i, err := strconv.Atoi(f.Name())
		if err != nil {
			return nil, fmt.Errorf("failed extracting shard number: %w", err)
		}
		if i < 0 {
			return nil, fmt.Errorf("negative shard number %v", i)
		}
		shards = append(shards, uint32(i))
	}

	return shards, nil
}
