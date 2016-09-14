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
	rtopts    retention.Options
	client    client.AdminClient
	recordFn  recordFn
	logger    xlog.Logger
	scope     tally.Scope
	nowFn     clock.NowFn
	blockSize time.Duration
}

func newShardRepairer(opts Options, rpopts repair.Options) databaseShardRepairer {
	iopts := opts.InstrumentOptions()
	rtopts := opts.RetentionOptions()
	client := rpopts.AdminClient()

	r := shardRepairer{
		opts:      opts,
		rtopts:    rtopts,
		client:    client,
		logger:    iopts.Logger(),
		scope:     iopts.MetricsScope().SubScope("database.repair"),
		nowFn:     opts.ClockOptions().NowFn(),
		blockSize: rtopts.BlockSize(),
	}
	r.recordFn = r.recordDifferences

	return r
}

func (r shardRepairer) Repair(namespace string, shard databaseShard) error {
	ctx := r.opts.ContextPool().Get()
	defer ctx.Close()

	session, err := r.client.DefaultAdminSession()
	if err != nil {
		return err
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
		return err
	}
	if err := metadata.AddPeerMetadata(peerIter); err != nil {
		return err
	}

	// NB(xichen): only record the differences for now
	r.recordFn(namespace, shard, metadata.Compare())

	return nil
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
	r.logger.WithFields(
		xlog.NewLogField("namespace", namespace),
		xlog.NewLogField("shard", shard.ID()),
		xlog.NewLogField("numSeries", diffRes.NumSeries),
		xlog.NewLogField("numBlocks", diffRes.NumBlocks),
	).Infof("repair total count")

	// Record size differences
	sizeDiffSeries := diffRes.SizeDifferences.Series()
	numBlocks := 0
	for _, series := range sizeDiffSeries {
		numBlocks += len(series.Blocks())
	}
	sizeDiffScope.Counter("series").Inc(int64(len(sizeDiffSeries)))
	sizeDiffScope.Counter("blocks").Inc(int64(numBlocks))
	r.logger.WithFields(
		xlog.NewLogField("namespace", namespace),
		xlog.NewLogField("shard", shard.ID()),
		xlog.NewLogField("numSeries", len(sizeDiffSeries)),
		xlog.NewLogField("numBlocks", numBlocks),
	).Infof("repair size difference count")

	// Record checksum differences
	checksumDiffSeries := diffRes.ChecksumDifferences.Series()
	numBlocks = 0
	for _, series := range checksumDiffSeries {
		numBlocks += len(series.Blocks())
	}
	checksumDiffScope.Counter("series").Inc(int64(len(checksumDiffSeries)))
	checksumDiffScope.Counter("blocks").Inc(int64(numBlocks))
	r.logger.WithFields(
		xlog.NewLogField("namespace", namespace),
		xlog.NewLogField("shard", shard.ID()),
		xlog.NewLogField("numSeries", len(checksumDiffSeries)),
		xlog.NewLogField("numBlocks", numBlocks),
	).Infof("repair checksum difference count")
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
	repairCheckInterval time.Duration
	closed              bool
}

func newDatabaseRepairer(database database) (databaseRepairer, error) {
	opts := database.Options()
	ropts := opts.RepairOptions()
	if ropts == nil {
		return nil, errNoRepairOptions
	}
	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	r := &dbRepairer{
		database:            database,
		opts:                opts,
		ropts:               ropts,
		shardRepairer:       newShardRepairer(opts, ropts),
		sleepFn:             time.Sleep,
		nowFn:               opts.ClockOptions().NowFn(),
		logger:              opts.InstrumentOptions().Logger(),
		repairInterval:      ropts.RepairInterval(),
		repairTimeOffset:    ropts.RepairTimeOffset(),
		repairCheckInterval: ropts.RepairCheckInterval(),
	}
	r.repairFn = r.Repair

	return r, nil
}

func (r *dbRepairer) Start() {
	if r.repairInterval <= 0 {
		return
	}

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
		if now.Sub(intervalStart) < r.repairTimeOffset {
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
