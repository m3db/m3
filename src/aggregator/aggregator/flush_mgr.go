// Copyright (c) 2017 Uber Technologies, Inc.
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

package aggregator

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uber-go/tally"
)

var (
	errBucketNotFound  = errors.New("bucket not found")
	errFlusherNotFound = errors.New("flusher not found")
)

// FlushManager manages and coordinates flushing activities across many
// periodic flushers with different flush intervals with controlled concurrency
// for flushes to minimize spikes in CPU load and reduce p99 flush latencies.
type FlushManager interface {
	// Reset resets the flush manager.
	Reset() error

	// Open opens the flush manager.
	Open() error

	// Status returns the flush status.
	Status() FlushStatus

	// Register registers a flusher with the flush manager.
	Register(flusher PeriodicFlusher) error

	// Unregister unregisters a flusher with the flush manager.
	Unregister(flusher PeriodicFlusher) error

	// Close closes the flush manager.
	Close() error
}

// FlushStatus is the flush status.
type FlushStatus struct {
	ElectionState ElectionState `json:"electionState"`
	CanLead       bool          `json:"canLead"`
}

// flushTask is a flush task.
type flushTask interface {
	Run()
}

// roleBasedFlushManager manages flushing data based on their elected roles.
type roleBasedFlushManager interface {
	// Open opens the manager.
	Open()

	// Init initializes the manager with buckets.
	Init(buckets []*flushBucket)

	// Prepare prepares for a flush.
	Prepare(buckets []*flushBucket) (flushTask, time.Duration)

	// OnBucketAdded is called when a new bucket is added.
	OnBucketAdded(bucketIdx int, bucket *flushBucket)

	// CanLead returns true if the manager can take over the leader role.
	CanLead() bool

	// Close closes the manager.
	Close()
}

var (
	errFlushManagerAlreadyOpenOrClosed = errors.New("flush manager is already open or closed")
	errFlushManagerNotOpenOrClosed     = errors.New("flush manager is not open or closed")
	errFlushManagerOpen                = errors.New("flush manager is open")
)

type flushManagerState int

const (
	flushManagerNotOpen flushManagerState = iota
	flushManagerOpen
	flushManagerClosed
)

type flushManager struct {
	sync.RWMutex
	sync.WaitGroup

	scope        tally.Scope
	checkEvery   time.Duration
	electionMgr  ElectionManager
	leaderOpts   FlushManagerOptions
	followerOpts FlushManagerOptions

	state         flushManagerState
	doneCh        chan struct{}
	buckets       []*flushBucket
	electionState ElectionState
	leaderMgr     roleBasedFlushManager
	followerMgr   roleBasedFlushManager
	sleepFn       sleepFn
}

// NewFlushManager creates a new flush manager.
func NewFlushManager(opts FlushManagerOptions) FlushManager {
	if opts == nil {
		opts = NewFlushManagerOptions()
	}
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()

	leaderMgrScope := scope.SubScope("leader")
	leaderMgrInstrumentOpts := instrumentOpts.SetMetricsScope(leaderMgrScope)
	leaderOpts := opts.SetInstrumentOptions(leaderMgrInstrumentOpts)

	followerMgrScope := scope.SubScope("follower")
	followerMgrInstrumentOpts := instrumentOpts.SetMetricsScope(followerMgrScope)
	followerOpts := opts.SetInstrumentOptions(followerMgrInstrumentOpts)

	mgr := &flushManager{
		scope:        scope,
		checkEvery:   opts.CheckEvery(),
		electionMgr:  opts.ElectionManager(),
		leaderOpts:   leaderOpts,
		followerOpts: followerOpts,
		sleepFn:      time.Sleep,
	}
	mgr.Lock()
	mgr.resetWithLock()
	mgr.Unlock()
	return mgr
}

func (mgr *flushManager) Reset() error {
	mgr.Lock()
	defer mgr.Unlock()

	switch mgr.state {
	case flushManagerNotOpen:
		return nil
	case flushManagerOpen:
		return errFlushManagerOpen
	default:
		mgr.resetWithLock()
		return nil
	}
}

func (mgr *flushManager) Open() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != flushManagerNotOpen {
		return errFlushManagerAlreadyOpenOrClosed
	}
	mgr.leaderMgr.Open()
	mgr.followerMgr.Open()
	mgr.state = flushManagerOpen
	if mgr.checkEvery > 0 {
		mgr.Add(1)
		go mgr.flush()
	}
	return nil
}

func (mgr *flushManager) Register(flusher PeriodicFlusher) error {
	mgr.Lock()
	defer mgr.Unlock()

	bucket, err := mgr.findOrCreateBucketWithLock(flusher)
	if err != nil {
		return err
	}
	bucket.Add(flusher)
	return nil
}

func (mgr *flushManager) Unregister(flusher PeriodicFlusher) error {
	mgr.Lock()
	defer mgr.Unlock()

	bucket, err := mgr.findBucketWithLock(flusher)
	if err != nil {
		return err
	}
	return bucket.Remove(flusher)
}

func (mgr *flushManager) Status() FlushStatus {
	mgr.RLock()
	electionState := mgr.electionState
	canLead := mgr.flushManagerWithLock().CanLead()
	mgr.RUnlock()

	return FlushStatus{
		ElectionState: electionState,
		CanLead:       canLead,
	}
}

func (mgr *flushManager) Close() error {
	mgr.Lock()
	if mgr.state != flushManagerOpen {
		mgr.Unlock()
		return errFlushManagerNotOpenOrClosed
	}
	mgr.state = flushManagerClosed
	close(mgr.doneCh)
	mgr.leaderMgr.Close()
	mgr.followerMgr.Close()
	mgr.Unlock()

	mgr.Wait()
	return nil
}

func (mgr *flushManager) resetWithLock() {
	mgr.state = flushManagerNotOpen
	mgr.doneCh = make(chan struct{})
	mgr.electionState = FollowerState
	mgr.leaderMgr = newLeaderFlushManager(mgr.doneCh, mgr.leaderOpts)
	mgr.leaderMgr.Init(mgr.buckets)
	mgr.followerMgr = newFollowerFlushManager(mgr.doneCh, mgr.followerOpts)
	mgr.followerMgr.Init(mgr.buckets)
}

func (mgr *flushManager) findOrCreateBucketWithLock(l PeriodicFlusher) (*flushBucket, error) {
	bucket, err := mgr.findBucketWithLock(l)
	if err == nil {
		return bucket, nil
	}
	if err != errBucketNotFound {
		return nil, err
	}
	flushInterval := l.FlushInterval()
	bucketScope := mgr.scope.SubScope("bucket").Tagged(map[string]string{
		"interval": flushInterval.String(),
	})
	bucket = newBucket(flushInterval, bucketScope)
	mgr.buckets = append(mgr.buckets, bucket)
	mgr.flushManagerWithLock().OnBucketAdded(len(mgr.buckets)-1, bucket)
	return bucket, nil
}

func (mgr *flushManager) findBucketWithLock(l PeriodicFlusher) (*flushBucket, error) {
	flushInterval := l.FlushInterval()
	for _, bucket := range mgr.buckets {
		if bucket.interval == flushInterval {
			return bucket, nil
		}
	}
	return nil, errBucketNotFound
}

// NB(xichen): apparently timer.Reset() is more difficult to use than I originally
// anticipated. For now I'm simply waking up every second to check for updates. Maybe
// when I have more time I'll spend a few hours to get timer.Reset() right and switch
// to a timer.Start + timer.Stop + timer.Reset + timer.After based approach.
func (mgr *flushManager) flush() {
	defer mgr.Done()

	for {
		mgr.RLock()
		state := mgr.state
		electionState := mgr.electionState
		mgr.RUnlock()
		if state == flushManagerClosed {
			return
		}

		// If the election state has changed, we need to switch the flush manager.
		newElectionState := mgr.checkElectionState()
		if electionState != newElectionState {
			mgr.Lock()
			mgr.electionState = newElectionState
			mgr.flushManagerWithLock().Init(mgr.buckets)
			mgr.Unlock()
		}

		mgr.RLock()
		flushTask, waitFor := mgr.flushManagerWithLock().Prepare(mgr.buckets)
		mgr.RUnlock()
		if flushTask != nil {
			flushTask.Run()
		}
		if waitFor > 0 {
			mgr.sleepFn(waitFor)
		}
	}
}

func (mgr *flushManager) checkElectionState() ElectionState {
	switch mgr.electionMgr.ElectionState() {
	case FollowerState:
		return FollowerState
	default:
		return LeaderState
	}
}

func (mgr *flushManager) flushManagerWithLock() roleBasedFlushManager {
	switch mgr.electionState {
	case FollowerState:
		return mgr.followerMgr
	case LeaderState:
		return mgr.leaderMgr
	default:
		// We should never get here.
		panic(fmt.Sprintf("unknown election state %v", mgr.electionState))
	}
}

// flushBucket contains all the registered lists for a given flush interval.
// NB(xichen): flushBucket is not thread-safe. It is protected by the lock
// in the flush manager.
type flushBucket struct {
	interval time.Duration
	flushers []PeriodicFlusher
	duration tally.Timer
}

func newBucket(interval time.Duration, scope tally.Scope) *flushBucket {
	return &flushBucket{
		interval: interval,
		duration: scope.Timer("duration"),
	}
}

func (b *flushBucket) Add(flusher PeriodicFlusher) {
	b.flushers = append(b.flushers, flusher)
}

func (b *flushBucket) Remove(flusher PeriodicFlusher) error {
	numFlushers := len(b.flushers)
	for i := 0; i < numFlushers; i++ {
		if b.flushers[i] == flusher {
			b.flushers[i] = b.flushers[numFlushers-1]
			b.flushers = b.flushers[:numFlushers-1]
			return nil
		}
	}
	return errFlusherNotFound
}
