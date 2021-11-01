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
	"math/rand"
	"sync"
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/x/clock"
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
	Register(flusher flushingMetricList) error

	// Unregister unregisters a flusher with the flush manager.
	Unregister(flusher flushingMetricList) error

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

	// OnFlusherAdded is called when a new flusher is added to an existing bucket.
	OnFlusherAdded(bucketIdx int, bucket *flushBucket, flusher flushingMetricList)

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

	scope         tally.Scope
	checkEvery    time.Duration
	jitterEnabled bool
	maxJitterFn   FlushJitterFn
	electionMgr   ElectionManager
	leaderOpts    FlushManagerOptions
	followerOpts  FlushManagerOptions

	state         flushManagerState
	doneCh        chan struct{}
	rand          *rand.Rand
	randFn        randFn
	buckets       flushBuckets
	electionState ElectionState
	leaderMgr     roleBasedFlushManager
	followerMgr   roleBasedFlushManager
	nowFn         clock.NowFn
	sleepFn       sleepFn
}

// NewFlushManager creates a new flush manager.
func NewFlushManager(opts FlushManagerOptions) FlushManager {
	if opts == nil {
		opts = NewFlushManagerOptions()
	}
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	nowFn := opts.ClockOptions().NowFn()
	rand := rand.New(rand.NewSource(nowFn().UnixNano()))

	leaderMgrScope := scope.SubScope("leader")
	leaderMgrInstrumentOpts := instrumentOpts.SetMetricsScope(leaderMgrScope)
	leaderOpts := opts.SetInstrumentOptions(leaderMgrInstrumentOpts)

	followerMgrScope := scope.SubScope("follower")
	followerMgrInstrumentOpts := instrumentOpts.SetMetricsScope(followerMgrScope)
	followerOpts := opts.SetInstrumentOptions(followerMgrInstrumentOpts)

	mgr := &flushManager{
		scope:         scope,
		checkEvery:    opts.CheckEvery(),
		jitterEnabled: opts.JitterEnabled(),
		maxJitterFn:   opts.MaxJitterFn(),
		electionMgr:   opts.ElectionManager(),
		leaderOpts:    leaderOpts,
		followerOpts:  followerOpts,
		rand:          rand,
		randFn:        rand.Int63n,
		nowFn:         nowFn,
		sleepFn:       time.Sleep,
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

func (mgr *flushManager) Register(flusher flushingMetricList) error {
	mgr.Lock()
	defer mgr.Unlock()

	bucket, bucketIdx, err := mgr.findOrCreateBucketWithLock(flusher)
	if err != nil {
		return err
	}
	bucket.Add(flusher)
	mgr.flushManagerWithLock().OnFlusherAdded(bucketIdx, bucket, flusher)
	return nil
}

func (mgr *flushManager) Unregister(flusher flushingMetricList) error {
	mgr.Lock()
	defer mgr.Unlock()

	bucketID := flusher.ID()
	bucket, _, err := mgr.buckets.FindBucket(bucketID)
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

func (mgr *flushManager) findOrCreateBucketWithLock(l flushingMetricList) (*flushBucket, int, error) {
	bucketID := l.ID()
	bucket, bucketIdx, err := mgr.buckets.FindBucket(bucketID)
	if err == nil {
		return bucket, bucketIdx, nil
	}
	if err != errBucketNotFound {
		return nil, 0, err
	}
	// Forwarded metric list has a fixed flush offset to ensure forwarded metrics are flushed
	// immediately after we have waited long enough, whereas a standard metric list has a flexible
	// flush offset to more evenly spread out the load during flushing.
	flushInterval := l.FlushInterval()
	flushOffset, ok := l.FixedFlushOffset()
	if !ok {
		flushOffset = mgr.computeFlushIntervalOffset(flushInterval)
	}
	scope := mgr.scope.SubScope("bucket").Tagged(map[string]string{
		"bucket-type": bucketID.listType.String(),
		"interval":    flushInterval.String(),
	})
	bucket = newBucket(bucketID, flushInterval, flushOffset, scope)
	mgr.buckets = append(mgr.buckets, bucket)
	mgr.flushManagerWithLock().OnBucketAdded(len(mgr.buckets)-1, bucket)
	return bucket, len(mgr.buckets) - 1, nil
}

func (mgr *flushManager) computeFlushIntervalOffset(flushInterval time.Duration) time.Duration {
	if !mgr.jitterEnabled {
		// If jittering is disabled, we compute the offset between the current time
		// and the aligned time and use that as the bucket offset.
		now := mgr.nowFn()
		alignedNow := now.Truncate(flushInterval)
		offset := now.Sub(alignedNow)
		return offset
	}

	// Otherwise the offset is determined based on jittering.
	maxJitter := mgr.maxJitterFn(flushInterval)
	jitterNanos := mgr.randFn(int64(maxJitter))
	return time.Duration(jitterNanos)
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

// flushBucket contains all the registered flushing metric lists for a given flush interval.
// NB(xichen): flushBucket is not thread-safe. It is protected by the lock in the flush manager.
type flushBucket struct {
	bucketID         metricListID
	interval         time.Duration
	offset           time.Duration
	flushers         []flushingMetricList
	duration         tally.Timer
	flushLag         tally.Histogram
	followerFlushLag tally.Histogram
}

func newBucket(
	bucketID metricListID,
	interval, offset time.Duration,
	scope tally.Scope,
) *flushBucket {
	histFn := func(name string) tally.Histogram {
		return scope.Histogram(name, tally.DurationBuckets{
			10 * time.Millisecond,
			500 * time.Millisecond,
			time.Second,
			2 * time.Second,
			5 * time.Second,
			10 * time.Second,
			15 * time.Second,
			20 * time.Second,
			25 * time.Second,
			30 * time.Second,
			35 * time.Second,
			40 * time.Second,
			45 * time.Second,
			60 * time.Second,
			90 * time.Second,
			120 * time.Second,
			150 * time.Second,
			180 * time.Second,
			210 * time.Second,
			240 * time.Second,
			300 * time.Second,
		})
	}

	return &flushBucket{
		bucketID:         bucketID,
		interval:         interval,
		offset:           offset,
		duration:         scope.Timer("duration"),
		flushLag:         histFn("flush-lag"),
		followerFlushLag: histFn("follower-flush-lag"),
	}
}

func (b *flushBucket) Add(flusher flushingMetricList) {
	b.flushers = append(b.flushers, flusher)
}

func (b *flushBucket) Remove(flusher flushingMetricList) error {
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

type flushBuckets []*flushBucket

func (buckets flushBuckets) FindBucket(bucketID metricListID) (*flushBucket, int, error) {
	for bucketIdx, bucket := range buckets {
		if bucket.bucketID == bucketID {
			return bucket, bucketIdx, nil
		}
	}
	return nil, 0, errBucketNotFound
}
