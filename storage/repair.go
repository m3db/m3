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

package storage

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
)

var (
	errNoRepairOptions = errors.New("no repair options")
)

type recordFn func(namespace string, shard databaseShard, diffRes repair.MetadataComparisonResult)

type shardRepairer struct {
	opts      Options
	rpopts    repair.Options
	rtopts    retention.Options
	client    client.AdminClient
	recordFn  recordFn
	logger    xlog.Logger
	scope     tally.Scope
	nowFn     clock.NowFn
	blockSize time.Duration
}

func newShardRepairer(opts Options, rpopts repair.Options) (databaseShardRepairer, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("database.repair").Tagged(map[string]string{"host": hostname})
	rtopts := opts.RetentionOptions()

	r := shardRepairer{
		opts:      opts,
		rpopts:    rpopts,
		rtopts:    rtopts,
		client:    rpopts.AdminClient(),
		logger:    iopts.Logger(),
		scope:     scope,
		nowFn:     opts.ClockOptions().NowFn(),
		blockSize: rtopts.BlockSize(),
	}
	r.recordFn = r.recordDifferences

	return r, nil
}

func (r shardRepairer) Options() repair.Options {
	return r.rpopts
}

func (r shardRepairer) Repair(namespace string, shard databaseShard) (repair.MetadataComparisonResult, error) {
	ctx := r.opts.ContextPool().Get()
	defer ctx.Close()

	session, err := r.client.DefaultAdminSession()
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	var (
		now      = r.nowFn()
		start    = now.Add(-r.rtopts.RetentionPeriod())
		end      = now.Add(-r.rtopts.BufferPast()).Add(-r.blockSize)
		origin   = session.Origin()
		replicas = session.Replicas()
	)

	metadata := repair.NewReplicaMetadataComparer(replicas)

	// Add local metadata
	localMetadata, _ := shard.FetchBlocksMetadata(ctx, math.MaxInt64, 0, true, true)
	localIter := block.NewFilteredBlocksMetadataIter(start, end, r.blockSize, localMetadata)
	metadata.AddLocalMetadata(origin, localIter)

	// Add peer metadata
	peerIter, err := session.FetchBlocksMetadataFromPeers(namespace, shard.ID(), start, end, r.blockSize)
	if err != nil {
		return repair.MetadataComparisonResult{}, err
	}
	if err := metadata.AddPeerMetadata(peerIter); err != nil {
		return repair.MetadataComparisonResult{}, err
	}

	metadataRes := metadata.Compare()

	// NB(xichen): only record the differences for now
	r.recordFn(namespace, shard, metadataRes)

	return metadataRes, nil
}

// TODO(xichen): log the actual differences once we have an idea of the magnitude of discrepancies
func (r shardRepairer) recordDifferences(
	namespace string,
	shard databaseShard,
	diffRes repair.MetadataComparisonResult,
) {
	var (
		shardScope = r.scope.Tagged(map[string]string{
			"namespace": namespace,
			"shard":     strconv.Itoa(int(shard.ID())),
		})
		totalScope        = shardScope.Tagged(map[string]string{"resultType": "total"})
		sizeDiffScope     = shardScope.Tagged(map[string]string{"resultType": "sizeDiff"})
		checksumDiffScope = shardScope.Tagged(map[string]string{"resultType": "checksumDiff"})
	)

	// Record total number of series and total number of blocks
	totalScope.Counter("series").Inc(diffRes.NumSeries)
	totalScope.Counter("blocks").Inc(diffRes.NumBlocks)

	// Record size differences
	sizeDiffScope.Counter("series").Inc(diffRes.SizeDifferences.NumSeries())
	sizeDiffScope.Counter("blocks").Inc(diffRes.SizeDifferences.NumBlocks())

	// Record checksum differences
	checksumDiffScope.Counter("series").Inc(diffRes.ChecksumDifferences.NumSeries())
	checksumDiffScope.Counter("blocks").Inc(diffRes.ChecksumDifferences.NumBlocks())
}

type repairFn func() error

type sleepFn func(d time.Duration)

type dbRepairer struct {
	sync.Mutex

	database      database
	opts          Options
	ropts         repair.Options
	shardRepairer databaseShardRepairer

	repairFn            repairFn
	sleepFn             sleepFn
	nowFn               clock.NowFn
	logger              xlog.Logger
	repairInterval      time.Duration
	repairTimeOffset    time.Duration
	repairTimeJitter    time.Duration
	repairCheckInterval time.Duration
	closed              bool
}

func newDatabaseRepairer(database database) (databaseRepairer, error) {
	opts := database.Options()
	nowFn := opts.ClockOptions().NowFn()
	ropts := opts.RepairOptions()
	if ropts == nil {
		return nil, errNoRepairOptions
	}
	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	shardRepairer, err := newShardRepairer(opts, ropts)
	if err != nil {
		return nil, err
	}

	var jitter time.Duration
	if repairJitter := ropts.RepairTimeJitter(); repairJitter > 0 {
		rand.Seed(nowFn().UnixNano())
		jitter = time.Duration(rand.Int63n(int64(repairJitter)))
	}

	r := &dbRepairer{
		database:            database,
		opts:                opts,
		ropts:               ropts,
		shardRepairer:       shardRepairer,
		sleepFn:             time.Sleep,
		nowFn:               nowFn,
		logger:              opts.InstrumentOptions().Logger(),
		repairInterval:      ropts.RepairInterval(),
		repairTimeOffset:    ropts.RepairTimeOffset(),
		repairTimeJitter:    jitter,
		repairCheckInterval: ropts.RepairCheckInterval(),
	}
	r.repairFn = r.Repair

	return r, nil
}

func (r *dbRepairer) run() {
	var curIntervalStart time.Time

	for {
		r.Lock()
		closed := r.closed
		r.Unlock()

		if closed {
			break
		}

		r.sleepFn(r.repairCheckInterval)

		now := r.nowFn()
		intervalStart := now.Truncate(r.repairInterval)

		// If we haven't reached the offset yet, skip
		target := intervalStart.Add(r.repairTimeOffset + r.repairTimeJitter)
		if now.Before(target) {
			continue
		}

		// If we are in the same interval, we must have already repaired, skip
		if intervalStart == curIntervalStart {
			continue
		}

		curIntervalStart = intervalStart
		if err := r.repairFn(); err != nil {
			r.logger.Errorf("error repairing database: %v", err)
		}
	}
}

func (r *dbRepairer) Start() {
	if r.repairInterval <= 0 {
		return
	}

	go r.run()
}

func (r *dbRepairer) Stop() {
	r.Lock()
	r.closed = true
	r.Unlock()
}

func (r *dbRepairer) Repair() error {
	// Don't attempt a repair if the database is not bootstrapped yet
	if !r.database.IsBootstrapped() {
		return nil
	}

	multiErr := xerrors.NewMultiError()
	namespaces := r.database.getOwnedNamespaces()
	for _, n := range namespaces {
		if err := n.Repair(r.shardRepairer); err != nil {
			detailedErr := fmt.Errorf("namespace %s failed to repair: %v", n.Name(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}

	return multiErr.FinalError()
}
