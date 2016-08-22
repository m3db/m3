package watch

import (
	"sync"

	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/log"
)

// SourcePollFn provides source data
type SourcePollFn func() (interface{}, error)

// Source polls data by calling SourcePollFn and notifies its watches on updates
type Source interface {
	xclose.SimpleCloser

	// Watch returns the value and an Watch
	Watch() (interface{}, Watch, error)
}

// NewSource returns a Source
func NewSource(poll SourcePollFn, logger xlog.Logger) Source {
	s := &source{
		poll:   poll,
		w:      NewWatchable(),
		logger: logger,
	}

	go s.run()
	return s
}

type source struct {
	sync.RWMutex

	poll   SourcePollFn
	w      Watchable
	logger xlog.Logger
	closed bool
}

func (s *source) run() {
	for !s.isClosed() {
		data, err := s.poll()
		if err != nil {
			s.logger.Errorf("error polling input source: %v", err)
			continue
		}
		s.w.Update(data)
	}
}

func (s *source) isClosed() bool {
	s.RLock()
	defer s.RUnlock()
	return s.closed
}

func (s *source) Close() {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.w.Close()
}

func (s *source) Watch() (interface{}, Watch, error) {
	return s.w.Watch()
}
