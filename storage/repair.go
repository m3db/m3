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
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
)

var (
	errNoRepairOptions = errors.New("no repair options")
)

type recordFn func(namespace string, shard databaseShard, diffRes repair.MetadataComparisonResult)

type shardRepairer struct {
	opts    Options
	rtOpts  retention.Options
	session client.AdminSession

	recordFn  recordFn
	logger    xlog.Logger
	scope     tally.Scope
	nowFn     clock.NowFn
	blockSize time.Duration
	origin    topology.Host
	replicas  int
}

func newShardRepairer(opts Options, rpOpts repair.Options) (databaseShardRepairer, error) {
	newSessionFn := rpOpts.NewAdminSessionFn()
	session, err := newSessionFn()
	if err != nil {
		return nil, err
	}

	iOpts := opts.InstrumentOptions()
	rtOpts := opts.RetentionOptions()

	r := shardRepairer{
		opts:    opts,
		rtOpts:  rtOpts,
		session: session,

		logger:    iOpts.Logger(),
		scope:     iOpts.MetricsScope().SubScope("database"),
		nowFn:     opts.ClockOptions().NowFn(),
		blockSize: rtOpts.BlockSize(),
		origin:    session.Origin(),
		replicas:  session.Replicas(),
	}
	r.recordFn = r.recordDifferences

	return r, nil
}

func (r shardRepairer) Repair(namespace string, shard databaseShard) error {
	ctx := r.opts.ContextPool().Get()
	defer ctx.Close()

	var (
		now   = r.nowFn()
		start = now.Add(-r.rtOpts.RetentionPeriod()).Truncate(r.blockSize)
		end   = now.Add(-r.rtOpts.BufferPast()).Add(-r.blockSize).Truncate(r.blockSize)
	)

	metadata := repair.NewReplicaMetadataComparer(r.replicas)

	// Add local metadata
	localMetadata, _ := shard.FetchBlocksMetadata(ctx, math.MaxInt64, 0, true, true)
	localIter := block.NewFilteredBlocksMetadataIter(start, end, r.blockSize, localMetadata)
	metadata.AddLocalMetadata(r.origin, localIter)

	// Add peer metadata
	peerIter, err := r.session.FetchBlocksMetadataFromPeers(namespace, shard.ID(), start, end, r.blockSize)
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
	totalScope.Counter("repair.series").Inc(diffRes.NumSeries)
	totalScope.Counter("repair.blocks").Inc(diffRes.NumBlocks)
	r.logger.WithFields(
		xlog.NewLogField("namespace", namespace),
		xlog.NewLogField("shard", shard.ID()),
		xlog.NewLogField("resultType", "total"),
	).Infof("numSeries=%d, numBlocks=%d", diffRes.NumSeries, diffRes.NumBlocks)

	// Record size differences
	sizeDiffSeries := diffRes.SizeDifferences.Series()
	numBlocks := 0
	for _, series := range sizeDiffSeries {
		numBlocks += len(series.Blocks())
	}
	sizeDiffScope.Counter("repair.series").Inc(int64(len(sizeDiffSeries)))
	sizeDiffScope.Counter("repair.blocks").Inc(int64(numBlocks))
	r.logger.WithFields(
		xlog.NewLogField("namespace", namespace),
		xlog.NewLogField("shard", shard.ID()),
		xlog.NewLogField("resultType", "sizeDiff"),
	).Infof("numSeries=%d, numBlocks=%d", len(sizeDiffSeries), numBlocks)

	// Record checksum differences
	checksumDiffSeries := diffRes.ChecksumDifferences.Series()
	numBlocks = 0
	for _, series := range checksumDiffSeries {
		numBlocks += len(series.Blocks())
	}
	checksumDiffScope.Counter("repair.series").Inc(int64(len(checksumDiffSeries)))
	checksumDiffScope.Counter("repair.blocks").Inc(int64(numBlocks))
	r.logger.WithFields(
		xlog.NewLogField("namespace", namespace),
		xlog.NewLogField("shard", shard.ID()),
		xlog.NewLogField("resultType", "checksumDiff"),
	).Infof("numSeries=%d, numBlocks=%d", len(checksumDiffSeries), numBlocks)
}

type repairFn func() error

type dbRepairer struct {
	sync.RWMutex

	database      database
	opts          Options
	ropts         repair.Options
	shardRepairer databaseShardRepairer

	repairFn            repairFn
	nowFn               clock.NowFn
	logger              xlog.Logger
	repairInterval      time.Duration
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
		nowFn:               opts.ClockOptions().NowFn(),
		logger:              opts.InstrumentOptions().Logger(),
		repairInterval:      ropts.RepairInterval(),
		repairCheckInterval: ropts.RepairCheckInterval(),
	}
	r.repairFn = r.Repair

	return r, nil
}

func (r *dbRepairer) Start() {
	if r.repairInterval <= 0 {
		return
	}

	start := r.nowFn()
	for {
		r.RLock()
		closed := r.closed
		r.RUnlock()

		if closed {
			break
		}

		end := r.nowFn()
		if end.Sub(start) < r.repairInterval {
			time.Sleep(r.repairCheckInterval)
			continue
		}

		if err := r.repairFn(); err != nil {
			r.logger.Errorf("error repairing database: %v", err)
		}

		start = r.nowFn()
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

	// Lazily create the shard repairer
	if r.shardRepairer == nil {
		repairer, err := newShardRepairer(r.opts, r.ropts)
		if err != nil {
			return err
		}
		r.shardRepairer = repairer
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
