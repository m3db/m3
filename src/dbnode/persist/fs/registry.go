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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/x/close"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/watch"
	"go.uber.org/zap"
)

const (
	notifyAndRemoveTimeout       = time.Minute
	notifyAndRemoveCheckInterval = time.Second
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

func (r *fileRegistry) Open(filePath string) (File, error) {
	entry, err := r.newFileStatusOrExisting(filePath)
	if err != nil {
		return nil, err
	}
	return entry.Open()
}

func (r *fileRegistry) OpenFile(filePath string, flag int, perm os.FileMode) (File, error) {
	entry, err := r.newFileStatusOrExisting(filePath)
	if err != nil {
		return nil, err
	}
	return entry.OpenFile(flag, perm)
}

func (r *fileRegistry) Remove(filePath string) error {
	pathAbs, err := filepath.Abs(filePath)
	if err != nil {
		return err
	}

	r.Lock()
	defer r.Unlock()

	status, ok := r.entries[pathAbs]
	if !ok {
		// Just fine, no files open.
		return os.Remove(pathAbs)
	}

	// Delete from the registry.
	delete(r.entries, pathAbs)

	// Notify open file holders that this file is being removed
	// and perform a delayed remove which will delete the file
	// after all references are released.
	go func() {
		err := status.NotifyAndRemove()
		if err != nil {
			r.logger.Error("unable to delayed remove file",
				zap.String("path", pathAbs), zap.Error(err))
		}
	}()

	return nil
}

func (r *fileRegistry) newFileStatusOrExisting(filePath string) (*fileStatus, error) {
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

	status, ok = r.entries[pathAbs]
	if !ok {
		status = newFileStatus(pathAbs, r.clockOpts.NowFn(), r.logger)
		r.entries[pathAbs] = status
	}

	return status, nil
}

type fileStatus struct {
	sync.RWMutex
	status    FileStatus
	pathAbs   string
	watchable watch.Watchable
	refs      []*fileRef

	nowFn clock.NowFn

	logger *zap.Logger
}

func newFileStatus(pathAbs string, nowFn clock.NowFn, logger *zap.Logger) *fileStatus {
	f := &fileStatus{
		status: FileStatus{
			Type: AvailableFileStatusType,
		},
		pathAbs: pathAbs,
		nowFn:   nowFn,
		logger:  logger,
	}
	watchable := watch.NewWatchable()
	watchable.Update(f.status)
	f.watchable = watchable
	return f
}

func (f *fileStatus) newFileRef(ref *os.File) *fileRef {
	fileRef := newFileRef(f, ref)
	f.Lock()
	f.refs = append(f.refs, fileRef)
	f.Unlock()
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

func (f *fileStatus) Status() FileStatus {
	f.RLock()
	v := f.status
	f.RUnlock()
	return v
}

func (f *fileStatus) NotifyAndRemove() error {
	f.setStatus(FileStatus{Type: RemovingFileStatusType})
	defer func() {
		f.setStatus(FileStatus{Type: RemovedFileStatusType})
	}()

	timeout := f.nowFn().Add(notifyAndRemoveTimeout)
	for f.nowFn().Before(timeout) && f.numRefs() > 0 {
		time.Sleep(notifyAndRemoveCheckInterval)
	}

	if f.numRefs() > 0 {
		f.logger.Error("timed out for delayed remove of file",
			zap.String("path", f.pathAbs))
	}

	return os.Remove(f.pathAbs)
}

func (f *fileStatus) setStatus(v FileStatus) {
	f.Lock()
	f.status = v
	f.watchable.Update(v)
	f.Unlock()
}

func (f *fileStatus) numRefs() int {
	f.RLock()
	v := len(f.refs)
	f.RUnlock()
	return v
}

func (f *fileStatus) Open() (File, error) {
	ref, err := os.Open(f.pathAbs)
	if err != nil {
		return nil, err
	}
	return f.newFileRef(ref), nil
}

func (f *fileStatus) OpenFile(flag int, perm os.FileMode) (File, error) {
	ref, err := os.OpenFile(f.pathAbs, flag, perm)
	if err != nil {
		return nil, err
	}
	return f.newFileRef(ref), nil
}

// Ensure fileRef implements File
var _ File = &fileRef{}

type fileRef struct {
	file *fileStatus
	ref  *os.File
}

func newFileRef(file *fileStatus, ref *os.File) *fileRef {
	return &fileRef{file: file, ref: ref}
}

func (f *fileRef) Status() FileStatus {
	return f.file.Status()
}

func (f *fileRef) PathAbs() string {
	return f.file.pathAbs
}

func (f *fileRef) WatchStatus(watcher FileStatusWatcher) close.SimpleCloser {
	_, watch, _ := f.file.watchable.Watch()

	// Spawn a new goroutine that will terminate when the
	// watchable terminates or the close of the watch.
	go func() {
		for range watch.C() {
			status := watch.Get().(FileStatus)
			watcher.OnFileStatusChange(f, status)
		}
	}()

	return watch
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
