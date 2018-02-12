package executor

import (
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/storage"
)

const (
	// DefaultQueryTimeout is the default timeout for executing a query.
	// A value of zero will have no query timeout.
	DefaultQueryTimeout = time.Duration(0)
)

// TaskStatus is the status of a query task
type TaskStatus int

const (
	// RunningTask is set when the task is running.
	RunningTask TaskStatus = iota

	// KilledTask is set when the task is killed, but resources are still
	// being used.
	KilledTask
)

func (t TaskStatus) String() string {
	switch t {
	case RunningTask:
		return "running"
	case KilledTask:
		return "killed"
	}
	panic(fmt.Sprintf("unknown task status: %d", int(t)))
}

// Tracker tracks the query state
type Tracker struct {
	// Log queries if they are slower than this time.
	// If zero, slow queries will never be logged.
	LogQueriesAfter time.Duration

	// Maximum number of concurrent queries.
	MaxConcurrentQueries int

	// Used for managing and tracking running queries.
	queries  map[uint64]*QueryTask
	nextID   uint64
	mu       sync.RWMutex
	shutdown bool
}

// QueryTask is the internal data structure for managing queries.
// For the public use data structure that gets returned, see QueryTask.
type QueryTask struct {
	qid       uint64
	query     string
	status    TaskStatus
	startTime time.Time
	closing   chan struct{}
	monitorCh chan error
	err       error
	mu        sync.Mutex
}

func (q *QueryTask) setError(err error) {
	q.mu.Lock()
	q.err = err
	q.mu.Unlock()
}

// NewTracker creates a new tracker.
func NewTracker() *Tracker {
	return &Tracker{
		queries:      make(map[uint64]*QueryTask),
		nextID:       1,
	}
}

// Track is used to add a new query to tracker
func (t *Tracker) Track(query storage.Query, connClosed <-chan bool) (*QueryTask, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	qid := t.nextID
	t.nextID++

	if t.shutdown {
		return nil, errors.ErrQueryEngineShutdown
	}

	if t.MaxConcurrentQueries > 0 && len(t.queries) >= t.MaxConcurrentQueries {
		return nil, errors.ErrMaxConcurrentQueriesLimitExceeded(len(t.queries), t.MaxConcurrentQueries)
	}

	queryTask := &QueryTask{
		query:     query.String(),
		status:    RunningTask,
		startTime: time.Now(),
		closing:   make(chan struct{}),
		monitorCh: make(chan error),
	}
	t.queries[qid] = queryTask

	go t.waitForQuery(qid, queryTask.closing, connClosed, queryTask.monitorCh)

	return queryTask, nil
}

func (t *Tracker) waitForQuery(qid uint64, queryClosed <-chan struct{}, connClosed <-chan bool, monitorCh <-chan error) {
	select {
	case <-connClosed:
		t.queryError(qid, errors.ErrQueryInterrupted)
	case err := <-monitorCh:
		if err == nil {
			break
		}

		t.queryError(qid, err)
	case <-queryClosed:
		// Query was manually closed so exit the select.
		return
	}

	// Stop the query execution
	t.KillQuery(qid)
}

// Close kills all running queries and prevents new queries from being attached.
func (t *Tracker) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.shutdown = true
	for _, query := range t.queries {
		query.setError(errors.ErrQueryEngineShutdown)
		close(query.closing)
	}
	t.queries = nil
	return nil
}

func (t *Tracker) queryError(qid uint64, err error) {
	t.mu.RLock()
	query := t.queries[qid]
	t.mu.RUnlock()
	if query != nil {
		query.setError(err)
	}
}

// KillQuery enters a query into the killed state and closes the channel
// from the TaskManager. This method can be used to forcefully terminate a
// running query.
func (t *Tracker) KillQuery(qid uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	query := t.queries[qid]
	if query == nil {
		return fmt.Errorf("no such query id: %d", qid)
	}

	close(query.closing)
	query.status = KilledTask
	return nil
}

// DetachQuery removes a query from the query table. If the query is not in the
// killed state, this will also close the related channel.
func (t *Tracker) DetachQuery(qid uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	query := t.queries[qid]
	if query == nil {
		return fmt.Errorf("no such query id: %d", qid)
	}

	if query.status != KilledTask {
		close(query.closing)
	}
	delete(t.queries, qid)
	return nil
}
