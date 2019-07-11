package writer

import (
	"sync"

	"github.com/m3db/m3/src/metrics/metric/aggregated"
)

type mutexWriter struct {
	sync.Mutex
	w Writer
}

// NewMutexWriter creates a writer with a lock
func NewMutexWriter(w Writer) Writer {
	return &mutexWriter{
		w: w,
	}
}

func (mw *mutexWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	mw.Lock()
	defer mw.Unlock()
	return mw.w.Write(mp)
}

func (mw *mutexWriter) Flush() error {
	mw.Lock()
	defer mw.Unlock()
	return mw.w.Flush()
}

func (mw *mutexWriter) Close() error {
	mw.Lock()
	defer mw.Unlock()
	return mw.w.Close()
}
