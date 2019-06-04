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

package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/m3db/m3/src/dbnode/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	"go.uber.org/zap"
)

var (
	errFileRemoved = errors.New("file is being removed, cannot open")
)

type fileRegistry struct {
	sync.RWMutex

	entries map[string]*fileStatus

	clockOpts clock.Options
	logger    *zap.Logger
}

// NewFileRegistry creates a new file registry.
func NewFileRegistry(
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) FileRegistry {
	return &fileRegistry{
		entries:   make(map[string]*fileStatus),
		clockOpts: clockOpts,
		logger:    instrumentOpts.Logger(),
	}
}

func (r *fileRegistry) Open(
	filePath string,
	leaser FileLeaser,
) (File, error) {
	entry, err := r.fileStatus(filePath)
	if err != nil {
		return nil, err
	}
	return entry.open(leaser)
}

func (r *fileRegistry) OpenFile(
	filePath string,
	flag int,
	perm os.FileMode,
	leaser FileLeaser,
) (File, error) {
	entry, err := r.fileStatus(filePath)
	if err != nil {
		return nil, err
	}
	return entry.openFile(flag, perm, leaser)
}

func (r *fileRegistry) Remove(filePath string) error {
	iter, err := NewFilesAbsolutePathIterator([]string{filePath})
	if err != nil {
		return err
	}
	return r.removeAllIter(iter)
}

func (r *fileRegistry) RemoveFiles(filePaths []string) error {
	iter, err := NewFilesAbsolutePathIterator(filePaths)
	if err != nil {
		return err
	}
	return r.removeAllIter(iter)
}

func (r *fileRegistry) RemoveAll(dirPath string) error {
	filePathsAbs, err := allDirPathAbs(dirPath)
	if err != nil {
		return err
	}

	iter := newFilesAbsolutePathIterator(filePathsAbs)
	return r.removeAllIter(iter)
}

func (r *fileRegistry) RemoveAllIter(iter FilesAbsolutePathsIterator) error {
	return r.removeAllIter(iter)
}

func (r *fileRegistry) removeAllIter(iter FilesAbsolutePathsIterator) error {
	var (
		removePathsAbsByLeaser = make(map[FileLeaser][]string)
		files                  []*fileStatus
	)

	r.Lock()

	// Save iter for cleanup.
	cleanupIter := iter.Duplicate()

	for iter.Next() {
		pathAbs := iter.CurrentPathAbs()

		// Mark file not as available while under lock
		// and ensure that no further leases can be made.
		status := r.fileStatusWithLock(pathAbs)
		status.setAccessError(errFileRemoved)

		// Track which files this leaser should remove at one time.
		for _, ref := range status.refs {
			paths, ok := removePathsAbsByLeaser[ref.leaser]
			if !ok {
				paths = []string{status.pathAbs}
			} else {
				paths = append(paths, status.pathAbs)
			}
			removePathsAbsByLeaser[ref.leaser] = paths
		}

		files = append(files, status)
	}

	r.Unlock()

	if err := xerrors.FirstError(iter.Err(), iter.Close()); err != nil {
		return err
	}

	// Wait for all leasers to remove files.
	var (
		multiErrLock sync.Mutex
		multiErr     xerrors.MultiError
		wg           sync.WaitGroup
	)
	for leaser, files := range removePathsAbsByLeaser {
		leaser, files := leaser, files // Capture for goroutine.
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := leaser.Release(newFilesAbsolutePathIterator(files))
			if err != nil {
				multiErrLock.Lock()
				multiErr = multiErr.Add(err)
				multiErrLock.Unlock()
			}
		}()
	}

	wg.Wait()

	if err := multiErr.FinalError(); err != nil {
		// Encountered error releasing files.
		return err
	}

	// Now double check all numRefs == 0
	for _, file := range files {
		if refs := file.numRefs(); refs != 0 {
			return fmt.Errorf(
				"released leases on file but still has ref: refs=%d, file=%v",
				refs, file.pathAbs)
		}
	}

	// Now since no one else able to open any refs, reqacquire lock and delete
	// from registry.
	r.Lock()
	defer r.Unlock()

	iter = cleanupIter
	for iter.Next() {
		pathAbs := iter.CurrentPathAbs()

		// Delete from the registry.
		delete(r.entries, pathAbs)
	}

	return xerrors.FirstError(iter.Err(), iter.Close())
}

func (r *fileRegistry) fileStatusWithLock(
	pathAbs string,
) *fileStatus {
	status, ok := r.entries[pathAbs]
	if !ok {
		status = newFileStatus(pathAbs, r.clockOpts.NowFn(), r.logger)
		r.entries[pathAbs] = status
	}
	return status
}

func (r *fileRegistry) fileStatus(
	filePath string,
) (*fileStatus, error) {
	pathAbs, err := filepath.Abs(filePath)
	if err != nil {
		return nil, err
	}

	r.RLock()
	status, ok := r.entries[pathAbs]
	r.RUnlock()

	if ok {
		return status, nil
	}

	r.Lock()
	defer r.Unlock()

	return r.fileStatusWithLock(pathAbs), nil
}

type fileStatus struct {
	sync.RWMutex
	pathAbs   string
	refs      []*fileRef
	leasers   []FileLeaser
	accessErr error

	nowFn clock.NowFn

	logger *zap.Logger
}

func newFileStatus(pathAbs string, nowFn clock.NowFn, logger *zap.Logger) *fileStatus {
	return &fileStatus{
		pathAbs: pathAbs,
		nowFn:   nowFn,
		logger:  logger,
	}
}

func (f *fileStatus) setAccessError(err error) {
	f.Lock()
	f.accessErr = err
	f.Unlock()
}

func (f *fileStatus) newFileRefWithLock(ref *os.File, leaser FileLeaser) *fileRef {
	fileRef := newFileRef(f, ref, leaser)
	f.refs = append(f.refs, fileRef)
	return fileRef
}

func (f *fileStatus) removeFileRef(elem *fileRef) error {
	f.Lock()
	defer f.Unlock()

	if len(f.refs) == 0 {
		return fmt.Errorf("remove file ref not found for: %v", elem.PathAbs())
	}

	refs := make([]*fileRef, 0, len(f.refs)-1)
	for _, ptr := range f.refs {
		if ptr != elem {
			refs = append(refs, ptr)
		}
	}
	if len(refs) != len(f.refs)-1 {
		return fmt.Errorf("remove file ref not found for: %v", elem.PathAbs())
	}

	f.refs = refs
	return nil
}

func (f *fileStatus) numRefs() int {
	f.RLock()
	v := len(f.refs)
	f.RUnlock()
	return v
}

func (f *fileStatus) open(leaser FileLeaser) (File, error) {
	f.Lock()
	defer f.Unlock()

	if err := f.accessErr; err != nil {
		return nil, err
	}

	ref, err := os.Open(f.pathAbs)
	if err != nil {
		return nil, err
	}

	return f.newFileRefWithLock(ref, leaser), nil
}

func (f *fileStatus) openFile(flag int, perm os.FileMode, leaser FileLeaser) (File, error) {
	f.Lock()
	defer f.Unlock()

	if err := f.accessErr; err != nil {
		return nil, err
	}

	ref, err := os.OpenFile(f.pathAbs, flag, perm)
	if err != nil {
		return nil, err
	}

	return f.newFileRefWithLock(ref, leaser), nil
}

// Ensure fileRef implements File.
var _ File = &fileRef{}

type fileRef struct {
	file   *fileStatus
	ref    *os.File
	leaser FileLeaser
}

func newFileRef(file *fileStatus, ref *os.File, leaser FileLeaser) *fileRef {
	return &fileRef{file: file, ref: ref, leaser: leaser}
}

func (f *fileRef) Equal(other File) bool {
	otherRef, ok := other.(*fileRef)
	if !ok {
		return false
	}
	return f.file == otherRef.file
}

func (f *fileRef) PathAbs() string {
	return f.file.pathAbs
}

func (f *fileRef) Close() error {
	err := f.ref.Close()
	if removeErr := f.file.removeFileRef(f); removeErr != nil && err == nil {
		err = removeErr
	}
	return err
}

func (f *fileRef) Fd() uintptr {
	return f.ref.Fd()
}

func (f *fileRef) Name() string {
	return f.ref.Name()
}

func (f *fileRef) Read(b []byte) (int, error) {
	return f.ref.Read(b)
}

func (f *fileRef) ReadAt(b []byte, off int64) (int, error) {
	return f.ref.ReadAt(b, off)
}

func (f *fileRef) Readdir(n int) ([]os.FileInfo, error) {
	return f.ref.Readdir(n)
}

func (f *fileRef) Readdirnames(n int) ([]string, error) {
	return f.ref.Readdirnames(n)
}

func (f *fileRef) Seek(offset int64, whence int) (ret int64, err error) {
	return f.ref.Seek(offset, whence)
}

func (f *fileRef) Stat() (os.FileInfo, error) {
	return f.ref.Stat()
}

func (f *fileRef) Sync() error {
	return f.ref.Sync()
}

func (f *fileRef) Write(b []byte) (int, error) {
	return f.ref.Write(b)
}

func (f *fileRef) WriteAt(b []byte, off int64) (int, error) {
	return f.ref.WriteAt(b, off)
}

type filesAbsolutePathsIterator struct {
	pathAbs []string
	index   int
}

// NewFilesAbsolutePathIterator takes a list of relative or absolute paths
// and returns an absolute file paths iterator.
func NewFilesAbsolutePathIterator(
	pathsRelativeOrAbsolute []string,
) (FilesAbsolutePathsIterator, error) {
	pathAbs := make([]string, 0, len(pathsRelativeOrAbsolute))
	for _, p := range pathsRelativeOrAbsolute {
		value, err := filepath.Abs(p)
		if err != nil {
			return nil, err
		}
		pathAbs = append(pathAbs, value)
	}
	return newFilesAbsolutePathIterator(pathAbs), nil
}

func newFilesAbsolutePathIterator(pathAbs []string) FilesAbsolutePathsIterator {
	return &filesAbsolutePathsIterator{pathAbs: pathAbs, index: -1}
}

func (i *filesAbsolutePathsIterator) Duplicate() FilesAbsolutePathsIterator {
	return newFilesAbsolutePathIterator(i.pathAbs)
}

func (i *filesAbsolutePathsIterator) Next() bool {
	return i.index < len(i.pathAbs)
}

func (i *filesAbsolutePathsIterator) CurrentPathAbs() string {
	return i.pathAbs[i.index]
}

func (i *filesAbsolutePathsIterator) Err() error {
	return nil
}

func (i *filesAbsolutePathsIterator) Close() error {
	return nil
}

func allDirPathAbs(dirPath string) ([]string, error) {
	dirPathAbs, err := filepath.Abs(dirPath)
	if err != nil {
		return nil, err
	}

	dir, err := os.Open(dirPathAbs)
	if err != nil {
		return nil, err
	}

	info, err := dir.Stat()
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("not a directory: %v", dirPathAbs)
	}

	filePaths, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(filePaths))
	for _, filePath := range filePaths {
		if filePath == "." || filePath == ".." {
			continue
		}

		filePathAbs, err := filepath.Abs(filePath)
		info, err := os.Stat(filePathAbs)
		if err != nil {
			return nil, err
		}

		if info.IsDir() {
			dirResults, err := allDirPathAbs(filePathAbs)
			if err != nil {
				return nil, err
			}
			result = append(result, dirResults...)
			continue
		}

		result = append(result, filePathAbs)
	}

	return result, nil
}
