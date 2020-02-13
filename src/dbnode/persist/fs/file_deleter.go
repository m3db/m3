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
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

// FileDeleter deletes files.
type FileDeleter interface {
	Delete(pattern string) error
}

// EfficientFileDeleter holds reusable memory for filenames to be deleted. This allows
// batch file deletions to be performedf with minimal memory usage.
type EfficientFileDeleter struct {
	fileNames []byte
}

// SimpleFileDeleter deletes files using native file deletion code.
type SimpleFileDeleter struct {
}

// NewEfficientFileDeleter returns a new deleter with a max allocation of bytes to use when deleting.
func NewEfficientFileDeleter(maxMemoryInBytes int) FileDeleter {
	return &EfficientFileDeleter{
		fileNames: make([]byte, maxMemoryInBytes),
	}
}

// NewSimpleFileDeleter returns a new deleter that performs standard native GO file deletion.
func NewSimpleFileDeleter() FileDeleter {
	return &SimpleFileDeleter{}
}

// Delete deletes all of the files that match the pattern.
func (d *SimpleFileDeleter) Delete(pattern string) error {
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	DeleteFiles(files)
	return nil
}

// Delete deletes all of the files that match the pattern.
func (d *EfficientFileDeleter) Delete(pattern string) error {
	dir, file := filepath.Split(pattern)
	openDir, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer openDir.Close()

	for i := 0; ; i++ {
		n, err := syscall.ReadDirent(int(openDir.Fd()), d.fileNames)
		if err != nil {
			return err
		}
		deleteMatchingFiles(dir, d.fileNames[:n], file)
		if n <= 0 {
			break
		}
	}

	return nil
}

func deleteMatchingFiles(dir string, fileNames []byte, filePattern string) {
	for len(fileNames) > 0 {
		reclen, ok := direntReclen(fileNames)
		if !ok || reclen > uint64(len(fileNames)) {
			return
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

		// TODO: consider implementing the wildcard pattern matching on the
		// raw bytes to avoid this yolo cast to call the Match func.
		nameStr := yoloString(name)
		matched, _ := filepath.Match(filePattern, nameStr)
		if !matched {
			continue
		}

		// Avoid actually allocating the string for the file path to delete until right before the actual delete
		// call. This ensures we only allocate what we absolutely must to call native file deletion. Note it is fine
		// that we allocate the directory string since there are very few directories and that gets shared per batch.
		var pathToDelete strings.Builder
		pathToDelete.WriteString(dir)
		pathToDelete.Write(name)
		os.Remove(pathToDelete.String())
	}
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

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
