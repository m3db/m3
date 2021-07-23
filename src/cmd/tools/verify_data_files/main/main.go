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

func main() {
	var (
		optPathPrefix          = getopt.StringLong("path-prefix", 'p', "/var/lib/m3db", "Path prefix [e.g. /var/lib/m3db]")
		optFailFast            = getopt.BoolLong("fail-fast", 'f', "Fail fast will bail on first failure")
		optFixDir              = getopt.StringLong("fix-path-prefix", 'o', "/tmp/m3db", "Fix output path file prefix for fixed files [e.g. /tmp/m3db]")
		optFixInvalidIDs       = getopt.BoolLong("fix-invalid-ids", 'i', "Fix invalid IDs will remove entries with IDs that have non-UTF8 chars")
		optFixInvalidTags      = getopt.BoolLong("fix-invalid-tags", 't', "Fix invalid tags will remove entries with tags that have name/values non-UTF8 chars")
		optFixInvalidChecksums = getopt.BoolLong("fix-invalid-checksums", 'c', "Fix invalid checksums will remove entries with bad checksums")
		optDebugLog            = getopt.BoolLong("debug", 'd', "Enable debug log level")
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

	log.Info("creating bytes pool")
	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	run(runOptions{
		filePathPrefix:      *optPathPrefix,
		failFast:            *optFailFast,
		fixDir:              *optFixDir,
		fixInvalidIDs:       *optFixInvalidIDs,
		fixInvalidTags:      *optFixInvalidTags,
		fixInvalidChecksums: *optFixInvalidChecksums,
		bytesPool:           bytesPool,
		log:                 log,
	})
}

type runOptions struct {
	filePathPrefix      string
	failFast            bool
	fixDir              string
	fixInvalidIDs       bool
	fixInvalidTags      bool
	fixInvalidChecksums bool
	bytesPool           pool.CheckedBytesPool
	log                 *zap.Logger
}

func run(opts runOptions) {
	filePathPrefix := opts.filePathPrefix
	bytesPool := opts.bytesPool
	log := opts.log

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
		if !fileSet.HasCompleteCheckpointFile() {
			continue // Don't validate file sets without checkpoint file.
		}

		log.Info("verifying file set file", zap.Any("fileSet", fileSet))
		if err := verifyFileSet(verifyFileSetOptions{
			filePathPrefix:      filePathPrefix,
			bytesPool:           bytesPool,
			fileSet:             fileSet,
			fixDir:              opts.fixDir,
			fixInvalidIDs:       opts.fixInvalidIDs,
			fixInvalidTags:      opts.fixInvalidTags,
			fixInvalidChecksums: opts.fixInvalidChecksums,
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

	results := entries[:0]
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

	fixDir              string
	fixInvalidIDs       bool
	fixInvalidTags      bool
	fixInvalidChecksums bool
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
		if err == nil {
			continue
		}

		shouldFixInvalidID := check.invalidID && opts.fixInvalidIDs
		shouldFixInvalidTags := check.invalidTags && opts.fixInvalidTags
		shouldFixInvalidChecksum := check.invalidChecksum && opts.fixInvalidChecksums
		if !shouldFixInvalidID && !shouldFixInvalidTags && !shouldFixInvalidChecksum {
			return err
		}

		log.Info("starting to fix file set", zap.Any("fileSet", fileSet))
		fixErr := fixFileSet(opts, log)
		if fixErr != nil {
			log.Error("could not fix file set",
				zap.Any("fileSet", fileSet), zap.Error(fixErr))
			return err
		}

		log.Info("fixed file set", zap.Any("fileSet", fileSet))
		return err
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
	idValue := id.Bytes()
	if len(idValue) == 0 {
		return readEntryResult{invalidID: true},
			fmt.Errorf("invalid id: err=%s, as_string=%s, as_hex=%x",
				"empty", idValue, idValue)
	}
	if !utf8.Valid(idValue) {
		return readEntryResult{invalidID: true},
			fmt.Errorf("invalid id: err=%s, as_string=%s, as_hex=%x",
				"non-utf8", idValue, idValue)
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

	success := false
	defer func() {
		if !success {
			writer.Close()
		}
	}()

	var (
		removedIDs  int
		removedTags int
		copies      []checked.Bytes
	)
	for {
		id, tags, data, checksum, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		tagsCopy := tags.Duplicate()

		check, err := readEntry(id, tags, data, checksum)
		if err != nil {
			shouldFixInvalidID := check.invalidID && opts.fixInvalidIDs
			shouldFixInvalidTags := check.invalidTags && opts.fixInvalidTags
			shouldFixInvalidChecksum := check.invalidChecksum && opts.fixInvalidChecksums
			log.Info("read entry for fix",
				zap.Bool("shouldFixInvalidID", shouldFixInvalidID),
				zap.Bool("shouldFixInvalidTags", shouldFixInvalidTags),
				zap.Bool("shouldFixInvalidChecksum", shouldFixInvalidChecksum))

			if shouldFixInvalidID || shouldFixInvalidTags || shouldFixInvalidChecksum {
				// Skip this entry being written to the target volume.
				removedIDs++
				continue
			}

			return fmt.Errorf("encountered an error not enabled to fix: %v", err)
		}

		metadata := persist.NewMetadataFromIDAndTagIterator(id, tagsCopy,
			persist.MetadataOptions{
				FinalizeID:          true,
				FinalizeTagIterator: true,
			})

		data.IncRef()
		err = writer.Write(metadata, data, checksum)
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

	success = true
	return writer.Close()
}
