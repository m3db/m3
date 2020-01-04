// Copyright (c) 2019 Uber Technologies, Inc.
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
	golog "log"
	"os"
	"path"
	"sort"
	"strconv"
	"unicode/utf8"

	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const snapshotType = "snapshot"
const flushType = "flush"

func main() {
	var (
		optPathPrefix     = getopt.StringLong("path-prefix", 'p', "/var/lib/m3db", "Path prefix [e.g. /var/lib/m3db]")
		optFailFast       = getopt.BoolLong("fail-fast", 'f', "Fail fast will bail on first failure")
		optFixDir         = getopt.StringLong("fix-path-prefix", 'o', "/tmp/m3db", "Fix output path file prefix for fixed files [e.g. /tmp/m3db]")
		optFixInvalidIDs  = getopt.BoolLong("fix-invalid-ids", 'i', "Fix invalid IDs will remove entries with IDs that have non-UTF8 chars")
		optFixInvalidTags = getopt.BoolLong("fix-invalid-tags", 't', "Fix invalid tags will remove tags with name/values non-UTF8 chars")
		optDebugLog       = getopt.BoolLong("debug", 'd', "Enable debug log level")
	)
	getopt.Parse()

	logLevel := zapcore.InfoLevel
	if *optDebugLog {
		logLevel = zapcore.DebugLevel
	}

	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level = zap.NewAtomicLevelAt(logLevel)
	log, err := logConfig.Build()
	if err != nil {
		golog.Fatalf("unable to create logger: %+v", err)
	}

	if *optPathPrefix == "" {
		getopt.Usage()
		os.Exit(1)
	}

	run(runOptions{
		filePathPrefix: *optPathPrefix,
		failFast:       *optFailFast,
		fixDir:         *optFixDir,
		fixInvalidIDs:  *optFixInvalidIDs,
		fixInvalidTags: *optFixInvalidTags,
		log:            log,
	})
}

type runOptions struct {
	filePathPrefix string
	failFast       bool
	fixDir         string
	fixInvalidIDs  bool
	fixInvalidTags bool
	log            *zap.Logger
}

func run(opts runOptions) {
	filePathPrefix := opts.filePathPrefix
	log := opts.log

	log.Info("creating bytes pool")
	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	dataDirPath := fs.DataDirPath(filePathPrefix)

	namespaces, err := dirFiles(dataDirPath)
	if err != nil {
		log.Fatal("could not read namespaces", zap.Error(err))
	}

	// Get all fileset files.
	log.Info("discovering file sets",
		zap.Strings("namespaces", namespaces))
	var fileSetFiles []fs.FileSetFile
	for _, namespace := range namespaces {
		namespacePath := path.Join(dataDirPath, namespace)
		shards, err := dirFiles(namespacePath)
		if err != nil {
			log.Fatal("could not read shards for namespace",
				zap.String("namespacePath", namespacePath),
				zap.Error(err))
		}

		log.Debug("discovered shards",
			zap.String("namespace", namespace),
			zap.String("namespacePath", namespacePath),
			zap.Strings("shards", shards))
		for _, shard := range shards {
			shardPath := path.Join(namespacePath, shard)
			shardID, err := strconv.Atoi(shard)
			if err != nil {
				log.Fatal("could not parse shard dir as int",
					zap.String("shardPath", shardPath), zap.Error(err))
			}

			shardFileSets, err := fs.DataFiles(filePathPrefix,
				ident.StringID(namespace), uint32(shardID))
			if err != nil {
				log.Fatal("could not list shard dir file setes",
					zap.String("shardPath", shardPath), zap.Error(err))
			}

			log.Debug("discovered shard file sets",
				zap.String("namespace", namespace),
				zap.String("namespacePath", namespacePath),
				zap.Int("shardID", shardID),
				zap.Any("fileSets", shardFileSets))
			fileSetFiles = append(fileSetFiles, shardFileSets...)
		}
	}

	// Sort by time in reverse (usually want to fix latest files first and
	// can stop once done with fail-fast).
	log.Info("sorting file sets", zap.Int("numFileSets", len(fileSetFiles)))
	sort.Slice(fileSetFiles, func(i, j int) bool {
		return fileSetFiles[i].ID.BlockStart.After(fileSetFiles[j].ID.BlockStart)
	})

	log.Info("verifying file sets", zap.Int("numFileSets", len(fileSetFiles)))
	for _, fileSet := range fileSetFiles {
		log.Info("verifying file set file", zap.Any("fileSet", fileSet))
		if err := verifyFileSet(verifyFileSetOptions{
			filePathPrefix: filePathPrefix,
			bytesPool:      bytesPool,
			fileSet:        fileSet,
			fixDir:         opts.fixDir,
			fixInvalidIDs:  opts.fixInvalidIDs,
			fixInvalidTags: opts.fixInvalidTags,
		}, log); err != nil {
			log.Error("file set file failed verification",
				zap.Error(err),
				zap.Any("fileSet", fileSet))

			if opts.failFast {
				log.Fatal("aborting due to fail fast set")
			}
		}
	}
}

func dirFiles(dirPath string) ([]string, error) {
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, fmt.Errorf("could not open dir: %v", err)
	}

	defer dir.Close()

	stat, err := dir.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not stat dir: %v", err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", dirPath)
	}

	entries, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, fmt.Errorf("could not read dir names: %v", err)
	}

	results := make([]string, 0, len(entries))
	for _, p := range entries {
		if p == "." || p == ".." || p == "./.." || p == "./" || p == "../" || p == "./../" {
			continue
		}
		results = append(results, p)
	}
	return results, nil
}

type verifyFileSetOptions struct {
	filePathPrefix string
	bytesPool      pool.CheckedBytesPool
	fileSet        fs.FileSetFile

	fixDir         string
	fixInvalidIDs  bool
	fixInvalidTags bool
}

func verifyFileSet(
	opts verifyFileSetOptions,
	log *zap.Logger,
) error {
	fsOpts := fs.NewOptions().SetFilePathPrefix(opts.filePathPrefix)
	reader, err := fs.NewReader(opts.bytesPool, fsOpts)
	if err != nil {
		return err
	}

	fileSet := opts.fileSet

	openOpts := fs.DataReaderOpenOptions{
		Identifier:  fileSet.ID,
		FileSetType: persist.FileSetFlushType,
	}

	err = reader.Open(openOpts)
	if err != nil {
		return err
	}

	defer reader.Close()

	for {
		id, tags, data, checksum, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		check, err := readEntry(id, tags, data, checksum)
		data.Finalize() // Always finalize data.
		if err != nil {
			shouldFixInvalidID := check.invalidID && opts.fixInvalidIDs
			shouldFixInvalidTags := check.invalidTags && opts.fixInvalidTags
			if shouldFixInvalidID || shouldFixInvalidTags {
				log.Info("starting to fix file set", zap.Any("fileSet", fileSet))
				fixErr := fixFileSet(opts, log)
				if fixErr == nil {
					log.Info("fixed file set", zap.Any("fileSet", fileSet))
					return err
				}

				log.Error("could not fix file set",
					zap.Any("fileSet", fileSet), zap.Error(fixErr))
			}
			return err
		}
	}

	return nil
}

type readEntryResult struct {
	invalidID       bool
	invalidTags     bool
	invalidChecksum bool
}

func readEntry(
	id ident.ID,
	tags ident.TagIterator,
	data checked.Bytes,
	checksum uint32,
) (readEntryResult, error) {
	if !utf8.Valid(id.Bytes()) {
		return readEntryResult{invalidID: true},
			fmt.Errorf("invalid id: err=%s, as_string=%s, as_hex=%x",
				"non-utf8", id.Bytes(), id.Bytes())
	}

	for tags.Next() {
		tag := tags.Current()
		if err := convert.ValidateSeriesTag(tag); err != nil {
			return readEntryResult{invalidTags: true},
				fmt.Errorf("invalid tag: err=%v, "+
					"name_as_string=%s, name_as_hex=%s"+
					"value_as_string=%s, value_as_hex=%s",
					err,
					tag.Name.Bytes(), tag.Name.Bytes(),
					tag.Value.Bytes(), tag.Value.Bytes())
		}
	}

	data.IncRef()
	calculatedChecksum := digest.Checksum(data.Bytes())
	data.DecRef()

	if calculatedChecksum != checksum {
		return readEntryResult{invalidChecksum: true},
			fmt.Errorf("data checksum invalid: actual=%v, expected=%v",
				calculatedChecksum, checksum)
	}
	return readEntryResult{}, nil
}

func fixFileSet(
	opts verifyFileSetOptions,
	log *zap.Logger,
) error {
	fsOpts := fs.NewOptions().SetFilePathPrefix(opts.filePathPrefix)
	reader, err := fs.NewReader(opts.bytesPool, fsOpts)
	if err != nil {
		return err
	}

	fileSet := opts.fileSet

	openOpts := fs.DataReaderOpenOptions{
		Identifier:  fileSet.ID,
		FileSetType: persist.FileSetFlushType,
	}

	err = reader.Open(openOpts)
	if err != nil {
		return err
	}

	defer reader.Close()

	// NOTE: we output to a new directory so that we don't clobber files.
	writeFsOpts := fsOpts.SetFilePathPrefix(opts.fixDir)
	writer, err := fs.NewWriter(writeFsOpts)
	if err != nil {
		return err
	}

	err = writer.Open(fs.DataWriterOpenOptions{
		FileSetType:        persist.FileSetFlushType,
		FileSetContentType: fileSet.ID.FileSetContentType,
		Identifier:         fileSet.ID,
		BlockSize:          reader.Status().BlockSize,
	})
	if err != nil {
		return err
	}

	var (
		currTags     []ident.Tag
		currTagsIter = ident.NewTagsIterator(ident.Tags{})
		tagsBuffer   []ident.Tag
		removedIDs   int
		removedTags  int
		copies       []checked.Bytes
	)
	for {
		id, tags, data, checksum, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Need to save tags in case we need to write them out again
		// (iterating them in read entry means we can't reiterate them
		// without copying/duplicating).
		currTags = currTags[:0]
		for tags.Next() {
			tag := tags.Current()
			name := tag.Name.Bytes()
			value := tag.Value.Bytes()

			// Need to take copy as only valid during iteration.
			nameCopy := opts.bytesPool.Get(len(name))
			nameCopy.IncRef()
			nameCopy.AppendAll(name)
			valueCopy := opts.bytesPool.Get(len(value))
			valueCopy.IncRef()
			valueCopy.AppendAll(value)
			copies = append(copies, nameCopy)
			copies = append(copies, valueCopy)

			currTags = append(currTags, ident.Tag{
				Name:  ident.BytesID(nameCopy.Bytes()),
				Value: ident.BytesID(valueCopy.Bytes()),
			})
		}

		// Choose to write out the current tags if do not need modifying.
		writeTags := currTags[:]

		var currIdentTags ident.Tags
		currIdentTags.Reset(currTags)
		currTagsIter.Reset(currIdentTags)

		check, err := readEntry(id, currTagsIter, data, checksum)
		if err != nil {
			shouldFixInvalidID := check.invalidID && opts.fixInvalidIDs
			if shouldFixInvalidID {
				// Skip this entry being written to the target volume.
				removedIDs++
				continue
			}

			shouldFixInvalidTags := check.invalidTags && opts.fixInvalidTags
			if shouldFixInvalidTags {
				// Need to remove invalid tags.
				tagsBuffer = tagsBuffer[:0]
				for _, tag := range currTags {
					if err := convert.ValidateSeriesTag(tag); err != nil {
						removedTags++
						continue
					}
					tagsBuffer = append(tagsBuffer, tag)
				}

				// Make sure we write out the modified tags.
				writeTags = tagsBuffer
			}
			log.Info("read entry for fix",
				zap.Bool("shouldFixInvalidID", shouldFixInvalidID),
				zap.Bool("shouldFixInvalidTags", shouldFixInvalidTags))

			if check.invalidChecksum {
				// Can't fix an invalid checksum.
				return fmt.Errorf("encountered an invalid checksum while fixing file set: %v", err)
			}
		}

		var writeIdentTags ident.Tags
		writeIdentTags.Reset(writeTags)

		data.IncRef()
		err = writer.Write(id, writeIdentTags, data, checksum)
		data.DecRef()
		if err != nil {
			return fmt.Errorf("could not write fixed file set entry: %v", err)
		}

		// Finalize data to release back to pool.
		data.Finalize()

		// Release our copies back to pool.
		for _, copy := range copies {
			copy.DecRef()
			copy.Finalize()
		}
		copies = copies[:0]
	}

	log.Info("finished fixing file set",
		zap.Any("fileSet", fileSet),
		zap.Int("removedIDs", removedIDs),
		zap.Int("removedTags", removedTags))

	return writer.Close()
}
