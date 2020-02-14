// Copyright (c) 2016 Uber Technologies, Inc.
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

package fs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unsafe"

	xerrors "github.com/m3db/m3/src/x/errors"
	xunsafe "github.com/m3db/m3/src/x/unsafe"
)

// FileDeleter deletes files.
type FileDeleter interface {
	Delete(directory string, filters ...FilterSetter) error
}

type filterConfig struct {
	patternFilter   *patternFilter
	olderThanFilter *olderThanFilter
}

type patternFilter struct {
	pattern string
}

type olderThanFilter struct {
	time         time.Time
	nameToTimeFn func(s string) (time.Time, error)
}

// FilterSetter configures are options to file to specific files to delete.
type FilterSetter func(*filterConfig)

// MatchesPattern filters to file names that match the pattern.
func MatchesPattern(pattern string) FilterSetter {
	return func(f *filterConfig) {
		f.patternFilter = &patternFilter{pattern: pattern}
	}
}

// OlderThan filters to files older than the provided time.
// A func must be provided to convert from filename to time.
func OlderThan(time time.Time, nameToTimeFn func(s string) (time.Time, error)) FilterSetter {
	return func(f *filterConfig) {
		f.olderThanFilter = &olderThanFilter{
			time:         time,
			nameToTimeFn: nameToTimeFn,
		}
	}
}

// EfficientFileDeleter holds reusable memory for filenames to be deleted. This allows
// batch file deletions to be performedf with minimal memory usage.
type EfficientFileDeleter struct {
	fileNames []byte
}

// SimpleFileDeleter deletes files using native file deletion code.
type SimpleFileDeleter struct {
}

// NewEfficientFileDeleter returns a new deleter that will avoid allocating unnecessary memory. Specifically
// it batch processes the matching of the filenames to be deleted in a fixed buffer with the provided filenameBatchByteSize.
// It then allocates memory only for the matched string filenames to be deleted, rather than all files in a given directory.
func NewEfficientFileDeleter(filenameBatchByteSize int) FileDeleter {
	return &EfficientFileDeleter{
		fileNames: make([]byte, filenameBatchByteSize),
	}
}

// NewSimpleFileDeleter returns a new deleter that performs standard native GO file deletion.
func NewSimpleFileDeleter() FileDeleter {
	return &SimpleFileDeleter{}
}

// Delete deletes all of the files that match the pattern.
func (d *SimpleFileDeleter) Delete(directory string, filters ...FilterSetter) error {
	if len(directory) == 0 {
		return errors.New("invalid empty directory name")
	}

	filterConfig := getFilterConfig(filters)

	// Default to * since that is the file catch-all for the Glob call.
	pattern := "*"
	if filterConfig.patternFilter != nil {
		pattern = filterConfig.patternFilter.pattern
	}

	// Read files based on pattern.
	files, err := filepath.Glob(filepath.Join(directory, pattern))
	if err != nil {
		return err
	}

	multiErr := xerrors.NewMultiError()
	for _, file := range files {
		// Filter out by time.
		if !filterConfig.olderThanFilter.passes(file) {
			continue
		}

		if err := os.Remove(file); err != nil {
			detailedErr := fmt.Errorf("failed to remove file %s: %v", file, err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}

// Delete deletes all of the files that match the pattern.
func (d *EfficientFileDeleter) Delete(directory string, filters ...FilterSetter) error {
	if len(directory) == 0 {
		return errors.New("invalid empty directory name")
	}

	openDir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer openDir.Close()

	var files []string
	filterConfig := getFilterConfig(filters)
	for i := 0; ; i++ {
		n, err := syscall.ReadDirent(int(openDir.Fd()), d.fileNames)
		if err != nil {
			return err
		}
		newFiles := getFileNamesToDelete(directory, d.fileNames[:n], filterConfig)
		files = append(files, newFiles...)
		if n <= 0 {
			break
		}
	}

	multiErr := xerrors.NewMultiError()
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			detailedErr := fmt.Errorf("failed to remove file %s: %v", file, err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}

func getFilterConfig(filters []FilterSetter) *filterConfig {
	config := &filterConfig{}
	for _, f := range filters {
		f(config)
	}
	return config
}

func (f *filterConfig) passesPatternFilter(name []byte) bool {
	// TODO: consider implementing the wildcard pattern matching on the
	// raw bytes to avoid this yolo cast to call the Match func.
	matched := false
	xunsafe.WithString(name, func(s string) {
		matched = f.patternFilter.passes(s)
	})
	return matched
}

func (f *filterConfig) passesOlderThanFilter(name []byte) bool {
	older := false
	xunsafe.WithString(name, func(s string) {
		older = f.olderThanFilter.passes(s)
	})
	return older
}

func (filter *olderThanFilter) passes(filename string) bool {
	if filter == nil {
		return true
	}
	time, _ := filter.nameToTimeFn(filename)
	return time.Before(filter.time)
}

func (filter *patternFilter) passes(filename string) bool {
	if filter == nil {
		return true
	}
	matched, _ := filepath.Match(filter.pattern, filename)
	return matched
}

func getFileNamesToDelete(dir string, fileNames []byte, filterConfig *filterConfig) []string {
	var namesToDelete []string
	for len(fileNames) > 0 {
		reclen, ok := direntReclen(fileNames)
		if !ok || reclen > uint64(len(fileNames)) {
			return namesToDelete
		}

		rec := fileNames[:reclen]
		fileNames = fileNames[reclen:]

		ino, ok := direntIno(rec)
		if !ok {
			break
		}
		if ino == 0 { // File absent in directory.
			continue
		}
		const namoff = uint64(unsafe.Offsetof(syscall.Dirent{}.Name))
		namlen, ok := direntNamlen(rec)
		if !ok || namoff+namlen > uint64(len(rec)) {
			break
		}
		name := rec[namoff : namoff+namlen]
		for i, c := range name {
			if c == 0 {
				name = name[:i]
				break
			}
		}

		if bytes.Equal(name, dotBytes) || bytes.Equal(name, doubleDotBytes) {
			// Check for useless names before allocating a string.
			continue
		}

		if !filterConfig.passesPatternFilter(name) {
			continue
		}

		if !filterConfig.passesOlderThanFilter(name) {
			continue
		}

		// Use only []byte instead of string until we know for sure we will delete the filename.
		// That way we avoid unnecessary string allocation for filenames we are not going to delete.
		var pathToDelete strings.Builder
		pathToDelete.WriteString(dir)
		if dir[len(dir)-1] != '/' {
			// Ensure slash between dir and filename.
			pathToDelete.WriteRune('/')
		}
		pathToDelete.Write(name)
		namesToDelete = append(namesToDelete, pathToDelete.String())
	}
	return namesToDelete
}

func direntReclen(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(syscall.Dirent{}.Reclen), unsafe.Sizeof(syscall.Dirent{}.Reclen))
}

func direntIno(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(syscall.Dirent{}.Ino), unsafe.Sizeof(syscall.Dirent{}.Ino))
}

func direntNamlen(buf []byte) (uint64, bool) {
	reclen, ok := direntReclen(buf)
	if !ok {
		return 0, false
	}
	return reclen - uint64(unsafe.Offsetof(syscall.Dirent{}.Name)), true
}

// readInt returns the size-bytes unsigned integer in native byte order at offset off.
func readInt(b []byte, off, size uintptr) (u uint64, ok bool) {
	if len(b) < int(off+size) {
		return 0, false
	}
	return readIntLE(b[off:], size), true
}

func readIntLE(b []byte, size uintptr) uint64 {
	switch size {
	case 1:
		return uint64(b[0])
	case 2:
		_ = b[1] // bounds check hint to compiler; see golang.org/issue/14808
		return uint64(b[0]) | uint64(b[1])<<8
	case 4:
		_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24
	case 8:
		_ = b[7] // bounds check hint to compiler; see golang.org/issue/14808
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
			uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
	default:
		panic("syscall: readInt with unsupported size")
	}
}
